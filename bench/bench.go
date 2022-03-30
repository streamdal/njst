package bench

import (
	"sync"

	"github.com/batchcorp/njst/cli"
	"github.com/batchcorp/njst/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	DefaultNumStreams       = 10
	DefaultNumReplicas      = 3
	DefaultBatchSize        = 1000
	DefaultMsgSizeBytes     = 4096
	DefaultNumMsgsPerStream = 10000
)

type Bench struct {
	params        *cli.Params
	producerMap   map[string]*Producer
	consumerMap   map[string]*Consumer
	producerMutex *sync.RWMutex
	consumerMutex *sync.RWMutex
	log           *logrus.Entry
}

type Producer struct {
	Settings *types.WriteSettings
}

type Consumer struct {
	Settings *types.ReadSettings
}

func New(p *cli.Params) (*Bench, error) {
	if err := validateParams(p); err != nil {
		return nil, errors.Wrap(err, "unable to validate params")
	}

	return &Bench{
		params:        p,
		producerMap:   make(map[string]*Producer),
		consumerMap:   make(map[string]*Consumer),
		producerMutex: &sync.RWMutex{},
		consumerMutex: &sync.RWMutex{},
		log:           logrus.WithField("pkg", "bench"),
	}, nil
}

func (b *Bench) Create(settings *types.Settings) (string, error) {

	return "", nil
}

func (b *Bench) Delete(id string) error {
	return nil
}

func (b *Bench) Status(id string) (*types.Status, error) {
	return nil, nil
}

func (b *Bench) createProducer(settings *types.Settings) (string, error) {
	if err := validateProducerSettings(settings); err != nil {
		return "", errors.Wrap(err, "unable to validate producer settings")
	}

	return "", nil
}

func (b *Bench) createConsumer(settings *types.Settings) (string, error) {
	if err := validateConsumerSettings(settings); err != nil {
		return "", errors.Wrap(err, "unable to validate consumer settings")
	}

	return "", nil
}

func (b *Bench) deleteProducer(id string) error {
	return nil
}

func (b *Bench) deleteConsumer(id string) error {
	return nil
}

func validateParams(p *cli.Params) error {
	if p == nil {
		return errors.New("params cannot be nil")
	}

	if p.NodeID == "" {
		return errors.New("node id cannot be empty")
	}

	if len(p.NATSAddress) == 0 {
		return errors.New("nats address cannot be empty")
	}

	return nil
}

func validateConsumerSettings(settings *types.Settings) error {
	if settings == nil {
		return errors.New("settings cannot be nil")
	}

	if settings.Read == nil {
		return errors.New("consumer settings cannot be nil")
	}

	return nil
}

func validateProducerSettings(settings *types.Settings) error {
	if settings == nil {
		return errors.New("settings cannot be nil")
	}

	if settings.Write == nil {
		return errors.New("producer settings cannot be nil")
	}

	return nil
}

func setDefaultProducerSettings(settings *types.WriteSettings) {
	if settings == nil {
		return
	}

	if settings.BatchSize == 0 {
		settings.BatchSize = DefaultBatchSize
	}

	if settings.MsgSizeBytes == 0 {
		settings.MsgSizeBytes = DefaultMsgSizeBytes
	}

	if settings.NumMsgsPerStream == 0 {
		settings.NumMsgsPerStream = DefaultNumMsgsPerStream
	}
}

func setDefaultConsumerSettings(settings *types.ReadSettings) {
	if settings == nil {
		return
	}

	if settings.BatchSize == 0 {
		settings.BatchSize = DefaultBatchSize
	}

	if settings.NumMsgsPerStream == 0 {
		settings.NumMsgsPerStream = DefaultNumMsgsPerStream
	}
}

func SetDefaultSettings(settings *types.Settings) {
	if settings == nil {
		return
	}

	if settings.NumStreams == 0 {
		settings.NumStreams = DefaultNumStreams
	}

	if settings.NumReplicas == 0 {
		settings.NumReplicas = DefaultNumReplicas
	}

	setDefaultProducerSettings(settings.Write)
	setDefaultConsumerSettings(settings.Read)

}
