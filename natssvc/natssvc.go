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
	"time"

	"github.com/batchcorp/njst/types"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/njst/cli"
)

const (
	HeartbeatBucket    = "njst-heartbeats"
	ResultBucketPrefix = "njst-results"
)

type NATSService struct {
	params     *cli.Params
	conn       *nats.Conn
	js         nats.JetStreamContext
	hkv        nats.KeyValue
	subs       map[string]*nats.Subscription
	subjectMap map[string]nats.MsgHandler
	log        *logrus.Entry
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
		subs:   make(map[string]*nats.Subscription),
		log:    logrus.WithField("pkg", "natssvc"),
	}

	n.subjectMap = map[string]nats.MsgHandler{
		fmt.Sprintf("njst.%s.create", params.NodeID): n.createHandler,
		fmt.Sprintf("njst.%s.delete", params.NodeID): n.deleteHandler,
		fmt.Sprintf("njst.%s.status", params.NodeID): n.statusHandler,
	}

	return n, nil
}

func (n *NATSService) Start() error {
	// Launch heartbeat
	n.log.Debug("launching heartbeat")

	go func() {
		if err := n.runHeartbeat(); err != nil {
			n.log.Errorf("heartbeat problem: %v", err)
		}

		n.log.Debug("heartbeat exiting")
	}()

	for subject, handler := range n.subjectMap {
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

		if err := n.conn.Publish(fmt.Sprintf("njst.%s.create", j.NodeID), data); err != nil {
			return errors.Wrapf(err, "unable to publish job '%s' for node '%s': %s", j.Settings.Name, j.NodeID, err)
		}
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
