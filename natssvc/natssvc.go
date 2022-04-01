package natssvc

import (
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/batchcorp/njst/types"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/njst/cli"
)

const (
	HeaderJobName = "job_name"

	HeartbeatBucket    = "njst-heartbeats"
	ResultBucketPrefix = "njst-results"
)

type NATSService struct {
	params       *cli.Params
	conn         *nats.Conn
	js           nats.JetStreamContext
	hkv          nats.KeyValue
	bucketsMutex *sync.RWMutex
	buckets      map[string]nats.KeyValue
	subs         map[string]*nats.Subscription
	subjectMap   map[string]nats.MsgHandler
	log          *logrus.Entry
}

func New(params *cli.Params) (*NATSService, error) {
	if err := validateParams(params); err != nil {
		return nil, errors.Wrap(err, "unable to validate params")
	}

	// Create conn
	c, err := newConn(params)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create new Nats client")
	}

	// Create JetStream context (for hkv)
	js, err := c.JetStream()
	if err != nil {
		return nil, errors.Wrap(err, "unable to create JetStream context")
	}

	// Do we need to create a heartbeat bucket?
	hkv, err := js.KeyValue(HeartbeatBucket)
	if err != nil {
		if err == nats.ErrBucketNotFound {
			// Create bucket
			hkv, err = js.CreateKeyValue(&nats.KeyValueConfig{
				Bucket:      HeartbeatBucket,
				Description: "njst heartbeat entries",
				TTL:         5 * time.Second,
			})

			if err != nil {
				return nil, errors.Wrap(err, "unable to create heartbeat bucket")
			}
		}

		return nil, errors.Wrap(err, "unable to determine heartbeat bucket status")
	}

	n := &NATSService{
		conn:   c,
		js:     js,
		hkv:    hkv,
		params: params,
		buckets: map[string]nats.KeyValue{
			HeartbeatBucket: hkv,
		},
		bucketsMutex: &sync.RWMutex{},
		subs:         make(map[string]*nats.Subscription),
		log:          logrus.WithField("pkg", "natssvc"),
	}

	return n, nil
}

func (n *NATSService) CacheBucket(name string, bucket nats.KeyValue) {
	n.bucketsMutex.Lock()
	defer n.bucketsMutex.Unlock()

	n.buckets[name] = bucket
}

func (n *NATSService) WriteStatus(status *types.Status) error {
	if status == nil {
		return errors.New("status cannot be nil")
	}

	if status.JobName == "" {
		return errors.New("job name cannot be empty")
	}

	bucket, err := n.GetOrCreateBucket(ResultBucketPrefix, status.JobName)
	if err != nil {
		return errors.Wrapf(err, "unable to get bucket for job '%s'", status.JobName)
	}

	data, err := json.Marshal(status)
	if err != nil {
		return errors.Wrapf(err, "unable to marshal status for job '%s'", status.JobName)
	}

	if _, err := bucket.Put(status.NodeID, data); err != nil {
		return errors.Wrapf(err, "unable to write status for job '%s'", status.JobName)
	}

	return nil
}

func (n *NATSService) GetOrCreateBucket(prefix, jobName string) (nats.KeyValue, error) {
	bucketName := fmt.Sprintf("%s-%s", prefix, jobName)

	n.bucketsMutex.RLock()
	bucket, ok := n.buckets[bucketName]
	n.bucketsMutex.RUnlock()

	if ok {
		return bucket, nil
	}

	// Bucket not in memory, see if we need to create it
	bucket, err := n.js.KeyValue(bucketName)
	if err != nil {
		if err == nats.ErrBucketNotFound {
			// Create bucket
			bucket, err = n.js.CreateKeyValue(&nats.KeyValueConfig{
				Bucket:      bucketName,
				Description: fmt.Sprintf("njst results for job '%s'", jobName),
			})

			if err != nil {
				return nil, errors.Wrapf(err, "unable to create bucket for job '%s'", jobName)
			}
		} else {
			return nil, errors.Wrapf(err, "unable to determine bucket status for job '%s'", jobName)
		}
	}

	n.bucketsMutex.Lock()
	n.buckets[bucketName] = bucket
	n.bucketsMutex.Unlock()

	return bucket, nil
}

func (n *NATSService) Start(msgHandlers map[string]nats.MsgHandler) error {
	// Launch heartbeat
	n.log.Debug("launching heartbeat")

	go func() {
		if err := n.runHeartbeat(); err != nil {
			n.log.Errorf("heartbeat problem: %v", err)
		}

		n.log.Debug("heartbeat exiting")
	}()

	for subject, handler := range msgHandlers {
		n.log.Debugf("subscribing to %s", subject)

		sub, err := n.conn.Subscribe(subject, handler)
		if err != nil {
			return errors.Wrapf(err, "unable to subscribe to subject '%s'", subject)
		}

		n.subs[subject] = sub
	}

	return nil
}

func (n *NATSService) runHeartbeat() error {
	var err error

	for {
		// Publish heartbeat
		_, err = n.hkv.Put(n.params.NodeID, []byte("I'm alive!"))
		if err != nil {
			n.log.Errorf("unable to write heartbeat kv: %s", err)
			break
		}

		time.Sleep(1 * time.Second)
	}

	n.log.Debug("heartbeat exiting")

	return err
}

func (n *NATSService) EmitJobs(jobs []*types.Job) error {
	if len(jobs) == 0 {
		return errors.New("jobs are empty - nothing to emit")
	}

	for _, j := range jobs {
		data, err := json.Marshal(j)
		if err != nil {
			return errors.Wrapf(err, "unable to marshal job '%s': %s", j.Settings.Name, err)
		}

		if err := n.conn.PublishMsg(&nats.Msg{
			Subject: fmt.Sprintf("njst.%s.create", j.NodeID),
			Header: map[string][]string{
				HeaderJobName: {j.Settings.Name},
			},
			Data: data,
		}); err != nil {
			return errors.Wrapf(err, "unable to publish job '%s' for node '%s': %s", j.Settings.Name, j.NodeID, err)
		}
	}

	return nil
}

func (n *NATSService) Publish(subject string, data []byte) error {
	if _, err := n.js.Publish(subject, data); err != nil {
		return errors.Wrapf(err, "unable to publish msg to subj '%s'", subject)
	}

	return nil
}

func (n *NATSService) RemoveHeartbeat() error {
	return n.hkv.Delete(n.params.NodeID)
}

func (n *NATSService) AddStream(streamConfig *nats.StreamConfig) (*nats.StreamInfo, error) {
	return n.js.AddStream(streamConfig)
}

// newConn creates a new Nats client connection
func newConn(params *cli.Params) (*nats.Conn, error) {
	_, err := url.Parse(params.NATSAddress[0])
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse NATS address")
	}

	if !params.NATSUseTLS {
		// Insecure connection
		c, err := nats.Connect(params.NATSAddress[0])
		if err != nil {
			return nil, errors.Wrap(err, "unable to create new nats client")
		}
		return c, nil
	}

	// TLS Secured connection
	tlsConfig, err := generateTLSConfig(
		params.NATSTLSCaCert,
		params.NATSTLSClientCert,
		params.NATSTLSClientKey,
		params.NATSTLSSkipVerify,
	)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to generate TLS Config")
	}

	c, err := nats.Connect(params.NATSAddress[0], nats.Secure(tlsConfig))
	if err != nil {
		return nil, errors.Wrap(err, "unable to create new nats client")
	}

	return c, nil
}

func validateParams(params *cli.Params) error {
	if params == nil {
		return errors.New("params cannot be nil")
	}

	if len(params.NATSAddress) == 0 {
		return errors.New("nats address cannot be empty or nil")
	}

	return nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func generateTLSConfig(caCert, clientCert, clientKey string, skipVerify bool) (*tls.Config, error) {
	certpool := x509.NewCertPool()

	if len(caCert) > 0 && fileExists(caCert) {
		// CLI input, read from file
		pemCerts, err := ioutil.ReadFile(caCert)
		if err == nil {
			certpool.AppendCertsFromPEM(pemCerts)
		}
	}

	// Import client certificate/key pair
	var cert tls.Certificate
	var err error

	if len(clientCert) > 0 && len(clientKey) > 0 {
		if fileExists(clientCert) {
			// CLI input, read from file
			cert, err = tls.LoadX509KeyPair(clientCert, clientKey)
			if err != nil {
				return nil, errors.Wrap(err, "unable to load client certificate")
			}
		}

		cert.Leaf, err = x509.ParseCertificate(cert.Certificate[0])
		if err != nil {
			return nil, errors.Wrap(err, "unable to parse certificate")
		}
	}

	// Create tls.Config with desired tls properties
	return &tls.Config{
		RootCAs:            certpool,
		ClientAuth:         tls.NoClientCert,
		ClientCAs:          nil,
		InsecureSkipVerify: skipVerify,
		Certificates:       []tls.Certificate{cert},
		MinVersion:         tls.VersionTLS12,
	}, nil
}

func (n *NATSService) GetNodeList() ([]string, error) {
	keys, err := n.hkv.Keys()
	if err != nil {
		return nil, errors.Wrap(err, "unable to get heartbeat keys")
	}

	return keys, nil
}

func RandID(length int) string {
	if length == 0 {
		length = 8
	}

	// fmt.Sprintf's %x prints two chars per byte
	length = length / 2

	b := make([]byte, length)

	if _, err := rand.Read(b); err != nil {
		panic(err)
	}

	return fmt.Sprintf("%x", b)
}
