package bench

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/batchcorp/njst/types"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	MaxErrorsPerWorker = 100
)

func (b *Bench) runReadBenchmark(job *types.Job) (*types.Status, error) {
	if job == nil || job.Settings == nil {
		return nil, errors.New("job or job settings cannot be nil")
	}

	doneCh := make(chan struct{}, 1)

	wg := &sync.WaitGroup{}

	if len(job.Settings.Read.Streams) == 0 {
		return nil, errors.New("no streams to read from")
	}

	workerMap := make(map[string]map[int]*Worker, 0)
	var (
		workerID int
		nc       *nats.Conn
	)

	if job.Settings.NATS.SharedConnection {
		var err error
		nc, err = b.nats.NewConn(job.Settings.NATS)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create single shared nats connection for job "+job.Settings.ID)
		}

		defer nc.Drain()
	}

	for _, streamInfo := range job.Settings.Read.Streams {
		for i := 0; i < job.Settings.Read.NumWorkersPerStream; i++ {
			if workerMap[streamInfo.StreamName] == nil {
				workerMap[streamInfo.StreamName] = make(map[int]*Worker, 0)
			}

			workerMap[streamInfo.StreamName][workerID] = &Worker{
				WorkerID: workerID,
				Errors:   make([]string, 0),
			}

			wg.Add(1)

			go b.runReaderWorker(job, nc, workerID, streamInfo, workerMap[streamInfo.StreamName][workerID], wg)

			workerID++
		}
	}

	// Launch periodic workerMap aggregation & reporting
	go b.runReporter(doneCh, job, workerMap)

	// Wait for all workers to finish
	wg.Wait()

	// Stop reporter & monitor
	close(doneCh)

	var status types.JobStatus

	// Was this job cancelled? (in select to avoid blocking if not cancelled)
	select {
	case _, ok := <-job.Context.Done():
		if !ok {
			status = types.CancelledStatus
		}
	default:
		status = types.CompletedStatus
	}

	// Calculate the final status
	return b.calculateStats(job.Settings, job.NodeID, workerMap, status, "; final"), nil
}

func (b *Bench) calculateNumRead(workerMap map[string]map[int]*Worker) map[string]int {
	numRead := make(map[string]int, 0)

	for stream, workers := range workerMap {
		for _, worker := range workers {
			numRead[stream] += worker.NumRead
		}
	}

	return numRead
}

func (b *Bench) runReaderWorker(job *types.Job, nc *nats.Conn, workerID int, streamInfo *types.StreamInfo, worker *Worker, wg *sync.WaitGroup) {
	var myNC = nc

	defer func() {
		wg.Done()
	}()

	llog := b.log.WithFields(logrus.Fields{
		"worker_id": workerID,
		"stream":    streamInfo.StreamName,
		"job_id":    job.Settings.ID,
	})

	llog.Debugf("worker starting")

	if !job.Settings.NATS.SharedConnection {
		var err error
		myNC, err = b.nats.NewConn(job.Settings.NATS)
		if err != nil {
			b.log.Log(logrus.ErrorLevel, "can't get connection for individual worker: ", err)
			return
		}

		defer myNC.Drain()
	} else if nc == nil {
		b.log.Error("worker not passed a valid shared NATS Connection")
		return
	}

	js, err := myNC.JetStream(nats.Context(job.Context))
	if err != nil {
		b.log.Log(logrus.ErrorLevel, "can't get JS context in ReaderWorker")
		return
	}

	sub, err := js.PullSubscribe(streamInfo.SubjectName, streamInfo.DurableName)
	if err != nil {
		llog.Errorf("unable to subscribe to stream '%s': %v", streamInfo.SubjectName, err)
		worker.Errors = append(worker.Errors, err.Error())
		worker.NumErrors++

		return
	}

	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			llog.Warningf("unable to unsubscribe from stream '%s': %v", streamInfo.StreamName, err)
		}
	}()

	targetNumberOfReads := (job.Settings.Read.NumMessagesPerStream / (job.Settings.Read.NumWorkersPerStream * job.Settings.Read.NumNodes)) / len(job.Settings.Read.Subjects)

	worker.StartedAt = time.Now().UTC()

	for worker.NumRead < targetNumberOfReads {
		llog.Debugf("worker has read %d messages out of %d", worker.NumRead, targetNumberOfReads)

		batchSize := func() int {
			if job.Settings.Read.BatchSize <= (targetNumberOfReads - worker.NumRead) {
				return job.Settings.Read.BatchSize
			} else {
				return targetNumberOfReads - worker.NumRead
			}
		}()

		msgs, err := sub.Fetch(batchSize, nats.Context(job.Context))
		if err != nil {
			if strings.Contains(err.Error(), "context canceled") {
				llog.Debug("worker asked to exit")

				break
			}

			if err == nats.ErrTimeout {
				llog.Warn("Fetch timeout")
			}
			pendingStr := "N/A"

			pending, _, pendingErr := sub.Pending()
			if pendingErr != nil {
				llog.Errorf("unable to get pending: %s", err)
			} else {
				pendingStr = fmt.Sprintf("%d", pending)
			}

			llog.Errorf("unable to fetch message(s) (pending: %s): %s", pendingStr, err)

			worker.NumErrors++

			if worker.NumErrors > MaxErrorsPerWorker {
				llog.Error("worker exiting prematurely due to too many errors")

				break
			}

			worker.Errors = append(worker.Errors, err.Error())

			continue
		}

		worker.NumRead += len(msgs)

		for _, msg := range msgs {
			// Do not pass ctx to Ack - it will cause the ack to be sync (and slow)
			if err := msg.Ack(); err != nil {
				llog.Warningf("unable to ack message: %s", err)
			}
		}
	}

	worker.EndedAt = time.Now().UTC()

	llog.Debugf("worker exiting; '%d' read, '%d' errors", worker.NumRead, worker.NumErrors)
}
