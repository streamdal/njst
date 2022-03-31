package bench

import (
	"fmt"
	"sync"
	"time"

	"github.com/batchcorp/njst/cli"
	"github.com/batchcorp/njst/natssvc"
	"github.com/batchcorp/njst/types"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	DefaultNumStreams            = 1
	DefaultNumReplicas           = 0
	DefaultBatchSize             = 100
	DefaultMsgSizeBytes          = 1024
	DefaultNumMessages           = 10000
	DefaultNumWorkers            = 1
	DefaultReadStrategy          = types.SpreadReadStrategy
	DefaultConsumerGroupStrategy = types.PerJobConsumerGroupStrategy
)

var (
	ValidReadStrategies = map[types.ReadStrategy]struct{}{
		types.SpreadReadStrategy: {},
		types.SharedReadStrategy: {},
	}

	ValidConsumerGroupStrategies = map[types.ConsumerGroupStrategy]struct{}{
		types.PerJobConsumerGroupStrategy:    {},
		types.PerStreamConsumerGroupStrategy: {},
		types.NoneConsumerGroupStrategy:      {},
	}
)

type Bench struct {
	nats          *natssvc.NATSService
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

func New(p *cli.Params, nsvc *natssvc.NATSService) (*Bench, error) {
	if err := validateParams(p); err != nil {
		return nil, errors.Wrap(err, "unable to validate params")
	}

	if nsvc == nil {
		return nil, errors.New("nats service cannot be nil")
	}

	return &Bench{
		params:        p,
		nats:          nsvc,
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

func (b *Bench) Status(name string) (*types.Status, error) {
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

func (b *Bench) createReadJobs(settings *types.Settings) ([]*types.Job, error) {
	// TODO: Do the streams exist?
	// TODO: Create consumers
	// TODO: Create job entries

	//for _, streamName := range streams {
	//	consumerGroupName := "cg-" + streamName
	//
	//	if _, err := n.js.AddConsumer(streamName, &nats.ConsumerConfig{
	//		Durable:     consumerGroupName,
	//		Description: "njst consumer",
	//	}); err != nil {
	//		return nil, errors.Wrapf(err, "unable to create consumer for stream '%s': %s", streamName, err)
	//	}
	//
	//	streams[streamName] = consumerGroupName
	//}
	return nil, nil
}

func (b *Bench) createWriteJobs(settings *types.Settings) ([]*types.Job, error) {
	if settings == nil || settings.Write == nil {
		return nil, errors.New("unable to setup write bench without write settings")
	}

	nodes, err := b.nats.GetNodeList()
	if err != nil {
		return nil, errors.Wrap(err, "unable to get node list")
	}

	if settings.Write.NumNodes > len(nodes) {
		return nil, errors.Errorf("unable to create write jobs: %d nodes requested but only %d available", settings.Write.NumNodes, len(nodes))
	}

	streamPrefix := fmt.Sprintf("njst-%s", settings.Name)

	// Create streams
	for i := 0; i < settings.Write.NumStreams; i++ {
		streamName := fmt.Sprintf("%s-%d", streamPrefix, i)

		if _, err := b.nats.AddStream(&nats.StreamConfig{
			Name:        streamName,
			Description: "njst bench stream",
			Subjects:    []string{streamName},
			Storage:     nats.MemoryStorage,
			Replicas:    settings.Write.NumReplicas,
		}); err != nil {
			return nil, errors.Wrapf(err, "unable to create stream '%s'", streamName)
		}
	}

	jobs := make([]*types.Job, 0)

	// How many nodes will this test run on?
	var numSelectedNodes int

	if settings.Write.NumNodes == 0 {
		numSelectedNodes = len(nodes)
	} else {
		numSelectedNodes = settings.Write.NumNodes
	}

	// How many streams per node?
	streamsPerNode := 1
	var streamsPerNodeRemainder int

	if settings.Write.NumStreams > numSelectedNodes {
		streamsPerNode = settings.Write.NumStreams / numSelectedNodes
		streamsPerNodeRemainder = settings.Write.NumStreams % numSelectedNodes
	}

	// How many messages per node?
	messagesPerNode := 1
	var messagesPerNodeRemainder int

	if settings.Write.NumMessages > numSelectedNodes {
		messagesPerNode = settings.Write.NumMessages / numSelectedNodes
		messagesPerNodeRemainder = settings.Write.NumMessages % numSelectedNodes
	}

	// Create jobs for nodes
	// Split the work equally across selected nodes
	lastIndex := 0

	for i := 0; i < numSelectedNodes; i++ {
		jobs = append(jobs, &types.Job{
			NodeID: nodes[i],
			Settings: &types.Settings{
				Name:        settings.Name,
				Description: settings.Description,
				Write: &types.WriteSettings{
					NumMessages:  messagesPerNode,
					NumWorkers:   settings.Write.NumWorkers,
					MsgSizeBytes: settings.Write.MsgSizeBytes,
					KeepStreams:  settings.Write.KeepStreams,
					Subjects:     generateSubjects(lastIndex, streamsPerNode, streamPrefix),
				},
			},
			CreatedBy: "todo-source-nodeid",
			CreatedAt: time.Now().UTC(),
		})

		lastIndex = lastIndex + streamsPerNode
	}

	// Add remainder messages to first node
	if messagesPerNodeRemainder > 0 {
		jobs[0].Settings.Write.NumMessages = jobs[0].Settings.Write.NumMessages + messagesPerNodeRemainder
	}

	// Add remainder streams to first node
	if streamsPerNodeRemainder > 0 {
		jobs[0].Settings.Write.Subjects = append(jobs[0].Settings.Write.Subjects, generateSubjects(lastIndex, streamsPerNodeRemainder, streamPrefix)...)
	}

	return jobs, nil
}

func generateSubjects(startIndex int, numSubjects int, subjectPrefix string) []string {
	subjects := make([]string, 0)

	for i := startIndex; i != numSubjects+startIndex; i++ {
		subjects = append(subjects, fmt.Sprintf("%s-%d", subjectPrefix, i))
	}

	return subjects
}

func (b *Bench) CreateJobs(settings *types.Settings) ([]*types.Job, error) {
	if settings == nil {
		return nil, errors.New("settings cannot be nil")
	}

	var err error
	var jobs []*types.Job

	if settings.Read != nil {
		jobs, err = b.createReadJobs(settings)
	} else if settings.Write != nil {
		jobs, err = b.createWriteJobs(settings)
	} else {
		return nil, errors.New("settings must have either read or write set")
	}

	if err != nil {
		return nil, errors.Wrap(err, "unable to create jobs")
	}

	return jobs, nil
}

func (b *Bench) deleteProducer(id string) error {
	return nil
}

func (b *Bench) deleteConsumer(id string) error {
	return nil
}

func (b *Bench) Exists(name string) (bool, error) {
	return false, nil
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
