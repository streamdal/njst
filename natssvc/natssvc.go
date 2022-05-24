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
	"strings"
	"sync"
	"time"

	"github.com/batchcorp/njst/types"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/njst/cli"
)

const (
	HeaderJobID = "job_id"

	HeartbeatBucket    = "njst-heartbeats"
	SettingsBucket     = "njst-settings"
	ResultBucketPrefix = "njst-results"
)

type NATSService struct {
	params       *cli.Params
	conn         *nats.Conn
	js           nats.JetStreamContext
	bucketsMutex *sync.RWMutex
	buckets      map[string]nats.KeyValue
	subs         map[string]*nats.Subscription
	subjectMap   map[string]nats.MsgHandler
	log          *logrus.Entry
}

type Bucket struct {
	Name        string
	Description string
	TTL         time.Duration
}

var (
	requiredBuckets = []Bucket{
		{
			Name:        HeartbeatBucket,
			Description: "Heartbeat bucket",
			TTL:         5 * time.Second,
		},
		{
			Name:        SettingsBucket,
			Description: "Settings bucket",
		},
	}
)

func New(params *cli.Params) (*NATSService, error) {
	if err := validateParams(params); err != nil {
		return nil, errors.Wrap(err, "unable to validate params")
	}

	// Create conn
	c, err := newConn(params)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create new Nats client")
	}

	// Create JetStream context (for kv)
	js, err := c.JetStream()
	if err != nil {
		return nil, errors.Wrap(err, "unable to create JetStream context")
	}

	internalBuckets := make(map[string]nats.KeyValue)

	// Create internal buckets
	for _, b := range requiredBuckets {
		kv, err := js.KeyValue(b.Name)
		if err != nil {
			if err == nats.ErrBucketNotFound {
				// Create bucket
				kv, err = js.CreateKeyValue(&nats.KeyValueConfig{
					Bucket:      b.Name,
					Description: b.Description,
					TTL:         b.TTL,
				})

				if err != nil {
					return nil, errors.Wrap(err, "unable to create heartbeat bucket")
				}
			} else {
				return nil, errors.Wrap(err, "unable to determine heartbeat bucket status")
			}
		}

		internalBuckets[b.Name] = kv
	}

	return &NATSService{
		conn:         c,
		js:           js,
		params:       params,
		buckets:      internalBuckets,
		bucketsMutex: &sync.RWMutex{},
		subs:         make(map[string]*nats.Subscription),
		log:          logrus.WithField("pkg", "natssvc"),
	}, nil
}

func (n *NATSService) NewConn(settings *types.NATS) (*nats.Conn, error) {
	if settings == nil {
		return nil, errors.New("settings cannot be nil")
	}

	return newConn(&cli.Params{
		NATSAddress: []string{
			settings.Address,
		},
	})
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

	if status.JobID == "" {
		return errors.New("job name cannot be empty")
	}

	bucket, err := n.GetOrCreateBucket(ResultBucketPrefix, status.JobID)
	if err != nil {
		return errors.Wrapf(err, "unable to get bucket for job '%s'", status.JobID)
	}

	data, err := json.Marshal(status)
	if err != nil {
		return errors.Wrapf(err, "> unable to marshal status for job '%s'", status.JobID)
	}

	if _, err := bucket.Put(status.NodeID, data); err != nil {
		return errors.Wrapf(err, "unable to write status for job '%s'", status.JobID)
	}

	return nil
}

func (n *NATSService) GetBucket(name string) (nats.KeyValue, error) {
	n.bucketsMutex.RLock()
	defer n.bucketsMutex.RUnlock()

	if bucket, ok := n.buckets[name]; ok {
		return bucket, nil
	}

	// Bucket not in mem, try to fetch it
	bucket, err := n.js.KeyValue(name)
	if err != nil {
		return nil, err
	}

	return bucket, nil
}

func (n *NATSService) GetOrCreateBucket(prefix, jobID string) (nats.KeyValue, error) {
	bucketName := fmt.Sprintf("%s-%s", prefix, jobID)

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
				Description: fmt.Sprintf("njst results for job '%s'", jobID),
			})

			if err != nil {
				return nil, errors.Wrapf(err, "unable to create bucket for job '%s'", jobID)
			}
		} else {
			return nil, errors.Wrapf(err, "unable to determine bucket status for job '%s'", jobID)
		}
	}

	n.bucketsMutex.Lock()
	n.buckets[bucketName] = bucket
	n.bucketsMutex.Unlock()

	return bucket, nil
}

func (n *NATSService) PullSubscribe(subj, consumerGroup string, opts ...nats.SubOpt) (*nats.Subscription, error) {
	return n.js.PullSubscribe(subj, consumerGroup, opts...)
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

	// Create subscriptions
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

	ticker := time.NewTicker(100 * time.Millisecond)

	for {
		<-ticker.C

		// Publish heartbeat
		_, err = n.buckets[HeartbeatBucket].Put(n.params.NodeID, []byte("I'm alive!"))
		if err != nil {
			n.log.Errorf("unable to write heartbeat kv: %s", err)
		}
	}
}

// GetStreams will get all defined streams. If a filter is provided, GetStreams
// will only return streams that match the filter.
func (n *NATSService) GetStreams(filter ...string) []string {
	var contains string

	// Only care about the first filter
	if len(filter) > 0 {
		contains = filter[0]
	}

	streams := make([]string, 0)

	for stream := range n.js.StreamNames() {
		if contains == "" {
			streams = append(streams, stream)
			continue
		} else {
			if strings.Contains(stream, contains) {
				streams = append(streams, stream)
			}
		}
	}

	return streams
}

func (n *NATSService) GetStreamInfo(stream string) (*nats.StreamInfo, error) {
	return n.js.StreamInfo(stream)
}

func (n *NATSService) EmitJobs(jobType types.JobType, jobs []*types.Job) error {
	if len(jobs) == 0 {
		return errors.New("jobs are empty - nothing to emit")
	}

	for _, j := range jobs {
		data, err := json.Marshal(j)
		if err != nil {
			return errors.Wrapf(err, "unable to marshal job '%s': %s", j.Settings.ID, err)
		}

		if err := n.conn.PublishMsg(&nats.Msg{
			Subject: fmt.Sprintf("njst.%s.%s", j.NodeID, jobType),
			Header: map[string][]string{
				HeaderJobID: {j.Settings.ID},
			},
			Data: data,
		}); err != nil {
			return errors.Wrapf(err, "unable to publish job '%s' for node '%s': %s", j.Settings.ID, j.NodeID, err)
		}
	}

	return nil
}

func (n *NATSService) DeleteSettings(id string) error {
	if err := n.buckets[SettingsBucket].Delete(id); err != nil {
		return errors.Wrapf(err, "unable to delete settings for job id '%s'", id)
	}

	return nil
}

func (n *NATSService) DeleteStreams(jobID string) error {
	for streamName := range n.js.StreamNames() {
		if strings.Contains(streamName, fmt.Sprintf("njst-%s-", jobID)) {
			if err := n.js.DeleteStream(streamName); err != nil {
				return errors.Wrapf(err, "unable to delete stream '%s'", streamName)
			}
		}
	}

	return nil
}

func (n *NATSService) DeleteResults(id string) error {
	if err := n.js.DeleteKeyValue(ResultBucketPrefix + "-" + id); err != nil {
		return errors.Wrapf(err, "unable to delete results for job id '%s'", id)
	}

	return nil
}

func (n *NATSService) Publish(subject string, data []byte, opts ...nats.PubOpt) error {
	if _, err := n.js.Publish(subject, data, opts...); err != nil {
		return errors.Wrapf(err, "unable to publish msg to subj '%s'", subject)
	}

	return nil
}

func (n *NATSService) RemoveHeartbeat() error {
	return n.buckets[HeartbeatBucket].Delete(n.params.NodeID)
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
	keys, err := n.buckets[HeartbeatBucket].Keys()
	if err != nil {
		return nil, errors.Wrap(err, "unable to get heartbeat keys")
	}

	return keys, nil
}

func (n *NATSService) SaveSettings(settings *types.Settings) error {
	data, err := json.Marshal(settings)
	if err != nil {
		return errors.Wrap(err, "unable to marshal settings to JSON")
	}

	if _, err := n.buckets[SettingsBucket].Put(settings.ID, data); err != nil {
		return errors.Wrap(err, "unable to save settings")
	}

	return nil
}

func (n *NATSService) GetSettings(id string) (*types.Settings, error) {
	entry, err := n.buckets[SettingsBucket].Get(id)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to get settings for id '%s'", id)
	}

	settings := &types.Settings{}

	if err := json.Unmarshal(entry.Value(), settings); err != nil {
		return nil, errors.Wrap(err, "unable to unmarshal settings from JSON")
	}

	return settings, nil
}

func (n *NATSService) AddDurableConsumer(stream string, cfg *nats.ConsumerConfig, opts ...nats.JSOpt) (*nats.ConsumerInfo, error) {
	return n.js.AddConsumer(stream, cfg, opts...)
}

func (n *NATSService) DeleteDurableConsumer(stream string) error {
	return n.js.DeleteConsumer(stream, stream+"-durable")
}

func (n *NATSService) GetAllSettings() ([]*types.Settings, error) {
	settingsList := make([]*types.Settings, 0)

	keys, err := n.buckets[SettingsBucket].Keys()
	if err != nil {
		if err == nats.ErrNoKeysFound {
			return settingsList, nil
		}

		return nil, errors.Wrap(err, "unable to get settings keys")
	}

	for _, key := range keys {
		settings, err := n.GetSettings(key)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to get settings for key '%s'", key)
		}

		settingsList = append(settingsList, settings)
	}

	return settingsList, nil
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
