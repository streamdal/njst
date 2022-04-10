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

type WorkerStats struct {
	WorkerID   int
	NumWritten int
	NumRead    int
	NumErrors  int
	Errors     []string
	StartedAt  time.Time
	EndedAt    time.Time
}

func (b *Bench) runWriteBenchmark(job *types.Job) (*types.Status, error) {
	stats := map[int]*WorkerStats{}

	if job == nil || job.Settings == nil {
		return nil, errors.New("job or job settings cannot be nil")
	}

	// Generate the data
	data, err := GenRandomBytes(job.Settings.Write.MsgSizeBytes)
	if err != nil {
		return nil, errors.Wrap(err, "unable to generate random data")
	}

	doneCh := make(chan struct{}, 1)

	go b.runReporter(doneCh, job, stats)

	wg := &sync.WaitGroup{}

	numMessagesPerWorker := job.Settings.Write.NumMessagesPerStream / job.Settings.Write.NumWorkersPerStream
	numMessagesPerLastWorker := numMessagesPerWorker + (job.Settings.Write.NumMessagesPerStream % job.Settings.Write.NumWorkersPerStream)

	// Launch workers; last one gets remainder
	for _, subj := range job.Settings.Write.Subjects {
		for i := 0; i < job.Settings.Write.NumWorkersPerStream; i++ {
			stats[i] = &WorkerStats{
				WorkerID:  i,
				StartedAt: time.Now(),
				Errors:    make([]string, 0),
			}

			wg.Add(1)

			// Last worker gets remaining messages
			if i == job.Settings.Write.NumWorkersPerStream-1 {
				go b.runWriterWorker(job.Context, i, subj, data, numMessagesPerLastWorker, stats[i], wg)
			} else {
				go b.runWriterWorker(job.Context, i, subj, data, numMessagesPerWorker, stats[i], wg)
			}
		}
	}

	// Wait for all workers to finish
	wg.Wait()

	// Stop the reporter
	doneCh <- struct{}{}

	// Calculate the final status
	return b.calculateStats(job.Settings, stats, types.CompletedStatus, "; final"), nil
}

func (b *Bench) runWriterWorker(ctx context.Context, workerID int, subj string, data []byte, numMessages int, stats *WorkerStats, wg *sync.WaitGroup) {
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

		// Avoiding a lock here to speed things up
		stats.NumWritten++
	}

	llog.Debug("worker exiting")

	stats.EndedAt = time.Now()
}

func (b *Bench) runReporter(doneCh chan struct{}, job *types.Job, stats map[int]*WorkerStats) {
	// Emit status every 5 seconds
	ticker := time.NewTicker(5 * time.Second)

MAIN:
	for {
		select {
		case <-job.Context.Done():
			b.log.Warningf("context canceled - exiting reporter")
			break MAIN
		case <-ticker.C:
			if err := b.nats.WriteStatus(b.calculateStats(job.Settings, stats, types.InProgressStatus, "; ticker")); err != nil {
				b.log.Error("unable to write status", "error", err)
			}
		case <-doneCh:
			break MAIN
		}
	}

	b.log.Debugf("reporter exiting for job '%s'", job.Settings.ID)
}

func (b *Bench) calculateStats(settings *types.Settings, stats map[int]*WorkerStats, jobStatus types.JobStatus, msg string) *types.Status {
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

	for _, status := range stats {
		elapsed := time.Now().Sub(status.StartedAt)

		if elapsed > maxElapsed {
			maxElapsed = elapsed
		}

		if status.StartedAt.After(maxStartedAt) {
			maxStartedAt = status.StartedAt
		}

		if status.EndedAt.After(maxEndedAt) {
			maxEndedAt = status.EndedAt
		}

		if settings.Read != nil {
			numProcessed += status.NumRead
		} else if settings.Write != nil {
			numProcessed += status.NumWritten
		}

		numErrorsTotal += status.NumErrors

		if len(status.Errors) > 0 {
			errs = append(errs, status.Errors...)
		}
	}

	// Cap of errors
	if len(errs) > 100 {
		errs = errs[:100]
	}

	avgMsgPerSec := float64(numProcessed) / maxElapsed.Seconds()

	return &types.Status{
		NodeID:         b.params.NodeID,
		Status:         jobStatus,
		Message:        message + msg,
		Errors:         errs,
		JobID:          settings.ID,
		ElapsedSeconds: maxElapsed.Seconds(),
		AvgMsgPerSec:   avgMsgPerSec,
		TotalProcessed: numProcessed,
		TotalErrors:    numErrorsTotal,
		StartedAt:      maxStartedAt,
		EndedAt:        maxEndedAt,
	}
}
