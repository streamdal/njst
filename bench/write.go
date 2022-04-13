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

	numMessagesPerWorker := job.Settings.Write.NumMessagesPerStream / job.Settings.Write.NumWorkersPerStream
	numMessagesPerLastWorker := numMessagesPerWorker + (job.Settings.Write.NumMessagesPerStream % job.Settings.Write.NumWorkersPerStream)

	// Launch workers; last one gets remainder
	for _, subj := range job.Settings.Write.Subjects {
		for i := 0; i < job.Settings.Write.NumWorkersPerStream; i++ {
			if _, ok := workerMap[subj]; !ok {
				workerMap[subj] = make(map[int]*Worker, 0)
			}

			workerMap[subj][i] = &Worker{
				WorkerID:  i,
				StartedAt: time.Now(),
				Errors:    make([]string, 0),
			}

			wg.Add(1)

			// Last worker gets remaining messages
			if i == job.Settings.Write.NumWorkersPerStream-1 {
				go b.runWriterWorker(job.Context, i, subj, data, numMessagesPerLastWorker, workerMap[subj][i], wg)
			} else {
				go b.runWriterWorker(job.Context, i, subj, data, numMessagesPerWorker, workerMap[subj][i], wg)
			}
		}
	}

	// Wait for all workers to finish
	wg.Wait()

	// Stop the reporter
	doneCh <- struct{}{}

	// Calculate the final status
	return b.calculateStats(job.Settings, workerMap, types.CompletedStatus, "; final"), nil
}

func (b *Bench) runWriterWorker(ctx context.Context, workerID int, subj string, data []byte, numMessages int, stats *Worker, wg *sync.WaitGroup) {
	defer wg.Done()

	llog := b.log.WithFields(logrus.Fields{
		"worker_id":   workerID,
		"subject":     subj,
		"numMessages": numMessages,
	})

	llog.Debug("worker starting")

	for i := 0; i < numMessages; i++ {
		err := b.nats.Publish(subj, data, nats.Context(ctx))
		if err != nil {
			if strings.Contains(err.Error(), "context canceled") {
				llog.Debug("worker context cancelled - '%d' published, '%d' errors", stats.NumWritten, stats.NumErrors)
				return
			}

			llog.Errorf("unable to publish message: %s", err)
			stats.NumErrors++

			if stats.NumErrors > numMessages {
				llog.Error("worker exiting prematurely due to too many errors")
				break
			}

			stats.Errors = append(stats.Errors, err.Error())
			continue
		}

		stats.NumWritten++
	}

	llog.Debugf("worker exiting; wrote '%d' messages", stats.NumWritten)

	stats.EndedAt = time.Now()
}

func (b *Bench) calculateStats(settings *types.Settings, workerMap map[string]map[int]*Worker, jobStatus types.JobStatus, msg string) *types.Status {
	var (
		maxElapsed     time.Duration
		maxStartedAt   time.Time
		maxEndedAt     time.Time
		numProcessed   int
		numErrorsTotal int
	)

	errs := make([]string, 0)

	message := "benchmark is in progress"

	if jobStatus == types.CompletedStatus {
		message = "benchmark completed"
	}

	for _, workGroup := range workerMap {
		for _, worker := range workGroup {
			elapsed := time.Now().Sub(worker.StartedAt)

			if elapsed > maxElapsed {
				maxElapsed = elapsed
			}

			if worker.StartedAt.After(maxStartedAt) {
				maxStartedAt = worker.StartedAt
			}

			if worker.EndedAt.After(maxEndedAt) {
				maxEndedAt = worker.EndedAt
			}

			if settings.Read != nil {
				numProcessed += worker.NumRead
			} else if settings.Write != nil {
				numProcessed += worker.NumWritten
			}

			numErrorsTotal += worker.NumErrors

			if len(worker.Errors) > 0 {
				errs = append(errs, worker.Errors...)
			}

		}
	}

	// Cap of errors
	if len(errs) > 100 {
		errs = errs[:100]
	}

	avgMsgPerSec := float64(numProcessed) / maxElapsed.Seconds()

	return &types.Status{
		NodeID:              b.params.NodeID,
		Status:              jobStatus,
		Message:             message + msg,
		Errors:              errs,
		JobID:               settings.ID,
		ElapsedSeconds:      maxElapsed.Seconds(),
		AvgMsgPerSecPerNode: avgMsgPerSec,
		TotalProcessed:      numProcessed,
		TotalErrors:         numErrorsTotal,
		StartedAt:           maxStartedAt,
		EndedAt:             maxEndedAt,
	}
}
