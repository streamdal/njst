package bench

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math"
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
	DefaultNumStreams           = 1
	DefaultBatchSize            = 100
	DefaultMsgSizeBytes         = 1024
	DefaultNumMessagesPerStream = 10000
	DefaultNumWorkersPerStream  = 1
	DefaultNumSubjects          = 1
)

type Bench struct {
	nats      *natssvc.NATSService
	params    *cli.Params
	jobs      map[string]*types.Job
	jobsMutex *sync.RWMutex
	log       *logrus.Entry
}

type Worker struct {
	WorkerID   int
	NumWritten int
	NumRead    int
	NumErrors  int
	Errors     []string
	StartedAt  time.Time
	EndedAt    time.Time
}

func New(p *cli.Params, nsvc *natssvc.NATSService) (*Bench, error) {
	if err := validateParams(p); err != nil {
		return nil, errors.Wrap(err, "unable to validate params")
	}

	if nsvc == nil {
		return nil, errors.New("nats service cannot be nil")
	}

	return &Bench{
		params:    p,
		nats:      nsvc,
		jobs:      make(map[string]*types.Job),
		jobsMutex: &sync.RWMutex{},
		log:       logrus.WithField("pkg", "bench"),
	}, nil
}

func (b *Bench) Purge(req *types.PurgeRequest) error {
	if req == nil {
		return errors.New("purge request cannot be nil")
	}

	if !req.All {
		return errors.New("'all' is false - won't purge")
	}

	// Get all jobs
	settings, err := b.nats.GetAllSettings()
	if err != nil {
		return errors.Wrap(err, "unable to fetch existing settings")
	}

	if len(settings) == 0 {
		b.log.Debugf("no settings found, not emitting jobs")
	} else {
		// Create delete jobs for each node
		jobs, err := b.GenerateDeleteAllJobs()
		if err != nil {
			return errors.Wrap(err, "unable to generate delete jobs for purge")
		}

		// Emit delete jobs
		if err := b.nats.EmitJobs(types.DeleteJob, jobs); err != nil {
			return errors.Wrap(err, "unable to emit delete jobs for purge")
		}
	}

	var errorCount int

	for _, cfg := range settings {
		// Delete settings
		if err := b.nats.DeleteSettings(cfg.ID); err != nil {
			b.log.Warningf("unable to delete settings for job '%s': %s", cfg.ID, err)
			errorCount++
		}

		// Delete results
		if err := b.nats.DeleteResults(cfg.ID); err != nil {
			b.log.Warningf("unable to delete results for job '%s': %s", cfg.ID, err)
			errorCount++
		}

		// Delete streams
		if err := b.nats.DeleteStreams(cfg.ID); err != nil {
			b.log.Warningf("unable to delete streams for job '%s': %s", cfg.ID, err)
			errorCount++
		}
	}

	b.log.Debugf("purge completed %d jobs, %d errors", len(settings), errorCount)

	return nil
}

func (b *Bench) Delete(jobID string, deleteStreams, deleteSettings, deleteResults bool) error {
	// Create delete jobs
	deleteJobs, err := b.GenerateDeleteJobs(jobID)
	if err != nil {
		return errors.Wrap(err, "unable to create delete jobs")
	}

	// Emit delete jobs
	if err := b.nats.EmitJobs(types.DeleteJob, deleteJobs); err != nil {
		return errors.Wrap(err, "unable to emit delete jobs")
	}

	// TODO: Wait for 3s to see if any errors are reported

	// Delete settings
	if deleteSettings {
		if err := b.nats.DeleteSettings(jobID); err != nil {
			return errors.Wrap(err, "unable to delete settings")
		}
	}

	// Delete results
	if deleteResults {
		if err := b.nats.DeleteResults(jobID); err != nil {
			return errors.Wrap(err, "unable to delete results")
		}
	}

	// Delete streams
	if deleteStreams {
		if err := b.nats.DeleteStreams(jobID); err != nil {
			return errors.Wrap(err, "unable to delete streams")
		}
	}

	return nil
}

func (b *Bench) runReporter(doneCh chan struct{}, job *types.Job, workerMap map[string]map[int]*Worker) {
	ticker := time.NewTicker(ReporterFrequency)
	llog := b.log.WithFields(logrus.Fields{
		"job": job.Settings.ID,
	})

MAIN:
	for {
		select {
		case <-job.Context.Done():
			llog.Debug("job aborted")
			break MAIN
		case <-doneCh:
			llog.Debug("job completed")
			break MAIN
		case <-ticker.C:
			aggregateStats := b.calculateStats(job.Settings, job.NodeID, workerMap, types.InProgressStatus, "; ticker")

			if err := b.nats.WriteStatus(aggregateStats); err != nil {
				b.log.Error("> unable to write status", err)
				b.log.Debugf("AGGREGATE: %+v", aggregateStats)

			}
		}
	}

	llog.Debug("reporter exiting")
}

func (b *Bench) Status(id string) (*types.Status, error) {
	fullBucketName := fmt.Sprintf("%s-%s", natssvc.ResultBucketPrefix, id)

	bucket, err := b.nats.GetBucket(fullBucketName)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get bucket")
	}

	keys, err := bucket.Keys()
	if err != nil {
		return nil, errors.Wrap(err, "unable to get keys")
	}

	finalStatus := &types.Status{}

	var totalPerNodeAverages = float64(0)
	var totalNumberOfNodesReporting = 0
	nodeReports := make([]*types.NodeReport, len(keys))

	for i, key := range keys {
		b.log.Debugf("looking up results in bucket '%s', object '%s'", fullBucketName, key)

		entry, err := bucket.Get(key)
		if err != nil {
			return nil, errors.Wrap(err, "unable to get k/v entry")
		}

		s := &types.Status{}

		if err := json.Unmarshal(entry.Value(), s); err != nil {
			return nil, errors.Wrap(err, "unable to unmarshal status")
		}

		finalStatus.JobID = s.JobID
		finalStatus.Message = s.Message
		finalStatus.TotalProcessed += s.TotalProcessed
		finalStatus.TotalErrors += s.TotalErrors
		totalPerNodeAverages += s.AvgMsgPerSecPerNode
		totalNumberOfNodesReporting++

		finalStatus.Status = s.Status

		if len(s.Errors) != 0 {
			finalStatus.Errors = append(finalStatus.Errors, s.Errors...)
		}

		if finalStatus.StartedAt.IsZero() {
			finalStatus.StartedAt = s.StartedAt
		}

		// Want to have the earliest start time
		if s.StartedAt.Before(finalStatus.StartedAt) {
			finalStatus.StartedAt = s.StartedAt
		}

		// Want to have the latest end time
		if s.EndedAt.After(finalStatus.EndedAt) {
			finalStatus.EndedAt = s.EndedAt
		}

		nodeReports[i] = &types.NodeReport{
			Streams: s.NodeReport.Streams,
		}
	}

	// Make stats more readable -- lower decimal point, deal with unfinished job
	if finalStatus.EndedAt.IsZero() {
		finalStatus.ElapsedSeconds = round(time.Now().UTC().Sub(finalStatus.StartedAt).Seconds(), 2)
	} else {
		finalStatus.ElapsedSeconds = round(finalStatus.EndedAt.Sub(finalStatus.StartedAt).Seconds(), 2)
	}

	finalStatus.TotalMsgPerSecAllNodes = round(totalPerNodeAverages, 2)
	finalStatus.AvgMsgPerSecPerNode = round(totalPerNodeAverages/float64(totalNumberOfNodesReporting), 2)

	finalStatus.NodeReports = nodeReports

	return finalStatus, nil
}

func (b *Bench) createProducer(settings *types.Settings) (string, error) {
	if err := validateProducerSettings(settings); err != nil {
		return "", errors.Wrap(err, "unable to validate producer settings")
	}

	return "", nil
}

// in order to be able to run a read bench multiple times in a row the durable consumer must be deleted between runs
// it's much easier to just delete the consumers (if they exist, they would not the first time the read bench is run,
// hence ignoring the error) right before re-creating them
func (b *Bench) deleteDurableConsumers(streams []string) {
	for _, stream := range streams {
		b.nats.DeleteDurableConsumer(stream)
	}
}

func (b *Bench) createDurableConsumers(settings *types.Settings, streams []string) ([]*types.StreamInfo, error) {
	if err := validateConsumerSettings(settings); err != nil {
		return nil, errors.Wrap(err, "unable to validate consumer settings")
	}

	streamInfo := make([]*types.StreamInfo, 0)

	for _, streamName := range streams {
		durableName := streamName + "-durable"

		if _, err := b.nats.AddDurableConsumer(streamName, &nats.ConsumerConfig{
			Durable:       durableName,
			Description:   "njst consumer",
			DeliverPolicy: nats.DeliverAllPolicy,
			AckPolicy:     nats.AckExplicitPolicy, // TODO: This should be configurable
			ReplayPolicy:  nats.ReplayInstantPolicy,
		}); err != nil {
			return nil, errors.Wrapf(err, "unable to create consumer group '%s' for stream '%s': %s",
				durableName, streamName, err)
		}

		streamInfo = append(streamInfo, &types.StreamInfo{
			StreamName:  streamName,
			DurableName: durableName,
		})
	}

	return streamInfo, nil
}

// Job logic
//
// 1. A stream is assigned to be worked on by 1 node.
//     * If there are more streams than nodes - individual streams are distributed
//       evenly between nodes.
//     * Streams are not "shared" between nodes
//
// 2. A job informs how many messages the job handler should process
//
// 3. For "read" jobs, settings.Read.Streams is used to inform the worker how
//    many streams the job should be reading from.
func (b *Bench) createReadJobs(settings *types.Settings) ([]*types.Job, error) {
	if settings == nil || settings.Read == nil {
		return nil, errors.New("unable to setup read bench without read settings")
	}

	nodes, err := b.nats.GetNodeList()
	if err != nil {
		return nil, errors.Wrap(err, "unable to get node list")
	}

	if settings.Read.NumNodes > len(nodes) {
		return nil, errors.Errorf("%d nodes requested but %d available", settings.Read.NumNodes, len(nodes))
	}

	if settings.Read.WriteID == "" {
		return nil, errors.New("existing write ID required for read bench")
	}

	// Do the streams exist?
	streams := b.nats.GetStreams("njst-" + settings.Read.WriteID + "-")

	if len(streams) < settings.Read.NumStreams {
		return nil, errors.Errorf("%d streams requested but %d available", settings.Read.NumStreams, len(streams))
	}

	if len(streams) > settings.Read.NumStreams {
		streams = streams[:settings.Read.NumStreams]
	}

	for _, stream := range streams {
		info, err := b.nats.GetStreamInfo(stream)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to get stream info for '%s' stream", stream)
		}

		// Do each of the streams have enough messages?
		if uint64(settings.Read.NumMessagesPerStream) > info.State.Msgs {
			return nil, fmt.Errorf("stream '%s' does not contain enough messages to satisfy read request", stream)
		}
		// Can we fit at least 1 batch per worker? <- Is this needed? Is batch best effort?
		// JNM: the answer is no you don't need it because the batch size passed to the Fetch() call is indeed a 'max number of messages returned back' (i.e. 'best effort')
	}

	jobs := make([]*types.Job, 0)

	// How many nodes will this test run on?
	var numSelectedNodes int

	if settings.Read.NumNodes == 0 {
		numSelectedNodes = len(nodes)
	} else {
		numSelectedNodes = settings.Read.NumNodes
	}

	b.deleteDurableConsumers(streams)

	streamInfo, err := b.createDurableConsumers(settings, streams)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create consumer")
	}

	for i := 0; i < numSelectedNodes; i++ {

		jobs = append(jobs, &types.Job{
			NodeID: nodes[i],
			Settings: &types.Settings{
				ID:          settings.ID,
				NATS:        settings.NATS,
				Description: settings.Description,
				Read: &types.ReadSettings{
					NumStreams:           settings.Read.NumStreams,
					NumNodes:             numSelectedNodes,
					NumMessagesPerStream: settings.Read.NumMessagesPerStream,
					NumWorkersPerStream:  settings.Read.NumWorkersPerStream,
					Streams:              streamInfo,
					BatchSize:            settings.Read.BatchSize,
				},
			},
			CreatedBy: b.params.NodeID,
			CreatedAt: time.Now().UTC(),
		})
	}

	return jobs, nil
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
		return nil, errors.Errorf("unable to create write jobs: %d nodes requested but %d available", settings.Write.NumNodes, len(nodes))
	}

	streamPrefix := fmt.Sprintf("njst-%s", settings.ID)

	storageType := nats.MemoryStorage

	if settings.Write.Storage == types.FileStorageType {
		storageType = nats.FileStorage
	}

	// Create streams
	for i := 0; i < settings.Write.NumStreams; i++ {
		streamName := fmt.Sprintf("%s-%d", streamPrefix, i)
		streamSubjects := make([]string, 0)

		for s := 0; s < settings.Write.NumSubjectsPerStream; s++ {
			streamSubjects = append(streamSubjects, fmt.Sprintf("%s.%d", streamName, s))
		}

		if _, err := b.nats.AddStream(&nats.StreamConfig{
			Name:        streamName,
			Description: "njst bench stream",
			Subjects:    streamSubjects,
			Storage:     storageType,
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

	settings.Write.NumNodes = numSelectedNodes

	for i := 0; i < numSelectedNodes; i++ {

		jobs = append(jobs, &types.Job{
			NodeID: nodes[i],
			Settings: &types.Settings{
				NATS:        settings.NATS,
				ID:          settings.ID,
				Description: settings.Description,
				Write: &types.WriteSettings{
					NumStreams:           settings.Write.NumStreams,
					NumNodes:             settings.Write.NumNodes,
					NumMessagesPerStream: settings.Write.NumMessagesPerStream,
					NumWorkersPerStream:  settings.Write.NumWorkersPerStream,
					MsgSizeBytes:         settings.Write.MsgSizeBytes,
					KeepStreams:          settings.Write.KeepStreams,
					NumSubjectsPerStream: settings.Write.NumSubjectsPerStream,
					Streams:              generateStreams(settings.Write.NumStreams, streamPrefix),
				},
			},
			CreatedBy: b.params.NodeID,
			CreatedAt: time.Now().UTC(),
		})
	}

	return jobs, nil
}

func generateStreams(numStreams int, streamPrefix string) []string {
	streams := make([]string, 0)

	for i := 0; i != numStreams; i++ {
		streams = append(streams, fmt.Sprintf("%s-%d", streamPrefix, i))
	}

	return streams
}

func (b *Bench) GenerateCreateJobs(settings *types.Settings) ([]*types.Job, error) {
	if settings == nil {
		return nil, errors.New("settings cannot be nil")
	}

	var err error
	var jobs []*types.Job

	if settings.Read != nil {
		jobs, err = b.createReadJobs(settings)
	} else if settings.Write != nil {
		jobs, err = b.createWriteJobs(settings)
		b.log.Debugf("%d write jobs created", len(jobs))

		for i, j := range jobs {
			b.log.Debugf("job #%d, nodes: %d, streams: %d, messages/stream: %d, workers/stream: %d",
				i, j.Settings.Write.NumNodes, j.Settings.Write.NumStreams, j.Settings.Write.NumMessagesPerStream, j.Settings.Write.NumWorkersPerStream)
		}
	} else {
		return nil, errors.New("settings must have either read or write set")
	}

	if err != nil {
		return nil, errors.Wrap(err, "unable to create jobs")
	}

	return jobs, nil
}

func (b *Bench) GenerateDeleteAllJobs() ([]*types.Job, error) {
	nodes, err := b.nats.GetNodeList()
	if err != nil {
		return nil, errors.Wrap(err, "unable to get node list")
	}

	jobs := make([]*types.Job, 0)

	// Get all settings
	settings, err := b.nats.GetAllSettings()
	if err != nil {
		return nil, errors.Wrap(err, "unable to fetch settings")
	}

	for _, node := range nodes {
		for _, cfg := range settings {
			jobs = append(jobs, &types.Job{
				NodeID: node,
				Settings: &types.Settings{
					ID: cfg.ID,
				},
				CreatedBy: "njst purge",
				CreatedAt: time.Now().UTC(),
			})
		}
	}

	return jobs, nil
}

func (b *Bench) GenerateDeleteJobs(id string) ([]*types.Job, error) {
	nodes, err := b.nats.GetNodeList()
	if err != nil {
		return nil, errors.Wrap(err, "unable to get node list")
	}

	jobs := make([]*types.Job, 0)

	for _, node := range nodes {
		jobs = append(jobs, &types.Job{
			NodeID: node,
			Settings: &types.Settings{
				ID: id,
			},
			CreatedBy: b.params.NodeID,
			CreatedAt: time.Now().UTC(),
		})
	}

	return jobs, nil
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

func GenRandomBytes(size int) ([]byte, error) {
	data := make([]byte, size)

	if _, err := rand.Read(data); err != nil {
		return nil, err
	}

	return data, nil
}

func round(f float64, places int) float64 {
	pow := math.Pow(10., float64(places))
	rounded := float64(int(f*pow)) / pow
	return rounded
}
