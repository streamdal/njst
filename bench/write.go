package bench

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/batchcorp/njst/types"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	ReporterFrequency = time.Second
)

func (b *Bench) runWriteBenchmark(job *types.Job) (*types.Status, error) {
	if job == nil || job.Settings == nil {
		return nil, errors.New("job or job settings cannot be nil")
	}

	workerMap := make(map[string]map[int]*Worker, 0)

	// Generate the data
	data, err := GenRandomBytes(job.Settings.Write.MsgSizeBytes)
	if err != nil {
		return nil, errors.Wrap(err, "unable to generate random data")
	}

	doneCh := make(chan struct{}, 1)

	go b.runReporter(doneCh, job, workerMap)

	wg := &sync.WaitGroup{}
	//var js nats.JetStreamContext

	numMessagesPerWorker := job.Settings.Write.NumMessagesPerStream / job.Settings.Write.NumWorkersPerStream
	numMessagesPerLastWorker := numMessagesPerWorker + (job.Settings.Write.NumMessagesPerStream % job.Settings.Write.NumWorkersPerStream)

	//if !job.Settings.NATS.ConnectionPerStream {
	nc, err := b.nats.NewConn(job.Settings.NATS)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to create nats connection for job %s", job.Settings.ID)
	}

	defer nc.Close()

	// Launch workers; last one gets remainder
	for _, subj := range job.Settings.Write.Subjects {
		//if job.Settings.NATS.ConnectionPerStream {
		//	nc, err := b.nats.NewConn(job.Settings.NATS)
		//	if err != nil {
		//		return nil, errors.Wrapf(err, "unable to create nats connection for subject %s", subj)
		//	}
		//
		//	defer nc.Close()
		//
		//	js, err = nc.JetStream(nats.PublishAsyncMaxPending(200))
		//	if err != nil {
		//		return nil, errors.Wrapf(err, "unable to create jetstream context for subject %s", subj)
		//	}
		//}

		for i := 0; i < job.Settings.Write.NumWorkersPerStream; i++ {
			if _, ok := workerMap[subj]; !ok {
				workerMap[subj] = make(map[int]*Worker, 0)
			}

			workerMap[subj][i] = &Worker{
				WorkerID: i,
				ch:       make(chan time.Time, 2),
				Errors:   make([]string, 0),
			}

			wg.Add(1)

			// Last worker gets remaining messages
			if i == job.Settings.Write.NumWorkersPerStream-1 {
				go b.runWriterWorker(job.Context, nc, job, i, subj, data, numMessagesPerLastWorker, workerMap[subj][i], wg)
			} else {
				go b.runWriterWorker(job.Context, nc, job, i, subj, data, numMessagesPerWorker, workerMap[subj][i], wg)
			}
		}
	}

	// Wait for all workers to finish
	wg.Wait()

	// Stop the reporter
	doneCh <- struct{}{}

	for stream := range workerMap {
		for worker := range workerMap[stream] {
			sa := <-workerMap[stream][worker].ch
			ea := <-workerMap[stream][worker].ch
			workerMap[stream][worker].StartedAt = sa
			workerMap[stream][worker].EndedAt = ea
		}
	}

	// Calculate the final status
	return b.calculateStats(job.Settings, workerMap, types.CompletedStatus, "; final"), nil
}

func (b *Bench) runWriterWorker(ctx context.Context, nc *nats.Conn, job *types.Job, workerID int, subj string, data []byte, numMessages int, worker *Worker, wg *sync.WaitGroup) {
	defer wg.Done()
	if job.Settings.NATS.ConnectionPerStream {
		nc, err := b.nats.NewConn(job.Settings.NATS)
		if err != nil {
			b.log.Log(logrus.ErrorLevel, "can't get connection for connection per stream: ", err)
			return
		}

		defer nc.Close()
	}
	js, err := nc.JetStream(nats.PublishAsyncMaxPending(200))
	if err != nil {
		b.log.Log(logrus.ErrorLevel, "can't get JS context in writeworker")
		return
	}

	llog := b.log.WithFields(logrus.Fields{
		"worker_id":   workerID,
		"subject":     subj,
		"numMessages": numMessages,
	})

	llog.Debug("worker starting")
	worker.ch <- time.Now()

	for i := 0; i < numMessages; i++ {
		//_, err := js.PublishAsync(subj, data, nats.Context(ctx))
		_, err := js.PublishAsync(subj, data)
		if err != nil {
			if strings.Contains(err.Error(), "context canceled") {
				llog.Debug("worker context cancelled - '%d' published, '%d' errors", worker.NumWritten, worker.NumErrors)
				return
			}

			llog.Errorf("unable to publish message: %s", err)
			worker.NumErrors++

			if worker.NumErrors > numMessages {
				llog.Error("worker exiting prematurely due to too many errors")
				break
			}

			worker.Errors = append(worker.Errors, err.Error())
			continue
		}

		worker.NumWritten++
	}

	worker.ch <- time.Now()
	llog.Debugf("worker exiting; wrote '%d' messages", worker.NumWritten)

}

func (b *Bench) calculateStats(settings *types.Settings, workerMap map[string]map[int]*Worker, jobStatus types.JobStatus, msg string) *types.Status {
	var (
		totalElapsed              time.Duration
		minStartedAt              time.Time
		maxEndedAt                time.Time
		numProcessedTotal         int
		numErrorsTotal            int
		totalPerWorkGroupAverages float64
	)

	errs := make([]string, 0)

	message := "benchmark is in progress"

	if jobStatus == types.CompletedStatus {
		message = "benchmark completed"
	}

	for _, workGroup := range workerMap {
		var workerTotalElapsed time.Duration = 0 * time.Second
		for _, worker := range workGroup {
			workerElapsed := worker.EndedAt.Sub(worker.StartedAt)
			workerTotalElapsed += workerElapsed

			workerNumProcessed := func() int {
				if settings.Read != nil {
					return worker.NumRead
				} else if settings.Write != nil {
					return worker.NumWritten
				}
				b.log.Error("Job settings has neither read or write")
				return 0
			}()

			numProcessedTotal += workerNumProcessed
			totalPerWorkGroupAverages += float64(workerNumProcessed) / workerElapsed.Seconds()

			if minStartedAt.IsZero() {
				minStartedAt = worker.StartedAt
			}

			if worker.StartedAt.Before(minStartedAt) {
				minStartedAt = worker.StartedAt
			}

			if worker.EndedAt.After(maxEndedAt) {
				maxEndedAt = worker.EndedAt
			}

			numErrorsTotal += worker.NumErrors

			if len(worker.Errors) > 0 {
				errs = append(errs, worker.Errors...)
			}
		}
		totalElapsed += workerTotalElapsed
	}

	// Cap of errors
	if len(errs) > 100 {
		errs = errs[:100]
	}

	avgMsgPerSec := totalPerWorkGroupAverages / float64(len(workerMap))

	return &types.Status{
		NodeID:              b.params.NodeID,
		Status:              jobStatus,
		Message:             message + msg,
		Errors:              errs,
		JobID:               settings.ID,
		ElapsedSeconds:      totalElapsed.Seconds(),
		AvgMsgPerSecPerNode: avgMsgPerSec,
		TotalProcessed:      numProcessedTotal,
		TotalErrors:         numErrorsTotal,
		StartedAt:           minStartedAt,
		EndedAt:             maxEndedAt,
	}
}
