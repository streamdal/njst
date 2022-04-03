package bench

import (
	"sync"
	"time"

	"github.com/batchcorp/njst/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type WriteStatus struct {
	WorkerID   int
	NumWritten int
	NumErrors  int
	Errors     []string
	StartedAt  time.Time
	EndedAt    time.Time
}

func (b *Bench) runWriteBenchmark(settings *types.Settings) (*types.Status, error) {
	stats := map[int]*WriteStatus{}

	// Generate the data
	data, err := GenRandomBytes(settings.Write.MsgSizeBytes)
	if err != nil {
		return nil, errors.Wrap(err, "unable to generate random data")
	}

	doneCh := make(chan struct{}, 1)

	go b.runReporter(doneCh, settings, stats)

	wg := &sync.WaitGroup{}

	numMessagesPerWorker := settings.Write.NumMessagesPerStream / settings.Write.NumWorkersPerStream
	numMessagesPerLastWorker := numMessagesPerWorker + (settings.Write.NumMessagesPerStream % settings.Write.NumWorkersPerStream)

	// Launch workers; last one gets remainder
	for _, subj := range settings.Write.Subjects {
		for i := 0; i < settings.Write.NumWorkersPerStream; i++ {
			stats[i] = &WriteStatus{
				WorkerID:  i,
				StartedAt: time.Now(),
				Errors:    make([]string, 0),
			}

			wg.Add(1)

			// Last worker gets remaining messages
			if i == settings.Write.NumWorkersPerStream-1 {
				go b.runWriterWorker(i, subj, data, numMessagesPerLastWorker, stats[i], wg)
			} else {
				go b.runWriterWorker(i, subj, data, numMessagesPerWorker, stats[i], wg)
			}
		}
	}

	// Wait for all workers to finish
	wg.Wait()

	// Stop the reporter
	doneCh <- struct{}{}

	// Calculate the final status
	return b.calculateWriteStats(settings, stats, types.CompletedStatus), nil
}

func (b *Bench) runWriterWorker(workerID int, subj string, data []byte, numMessages int, stats *WriteStatus, wg *sync.WaitGroup) {
	defer wg.Done()

	llog := b.log.WithFields(logrus.Fields{
		"worker_id":   workerID,
		"subject":     subj,
		"numMessages": numMessages,
	})

	llog.Debug("worker starting")

	for i := 0; i < numMessages; i++ {
		err := b.nats.Publish(subj, data)
		if err != nil {
			llog.Errorf("unable to publish message: %s", err)
			stats.NumErrors++
			stats.Errors = append(stats.Errors, err.Error())
			continue
		}

		// Avoiding a lock here to speed things up
		stats.NumWritten++
	}

	llog.Debug("worker exiting")

	stats.EndedAt = time.Now().Add(5 * time.Hour)
}

func (b *Bench) runReporter(doneCh chan struct{}, settings *types.Settings, stats map[int]*WriteStatus) {
	// Emit status every 5 seconds
	ticker := time.NewTicker(5 * time.Second)

MAIN:
	for {
		select {
		case <-ticker.C:
			if err := b.nats.WriteStatus(b.calculateWriteStats(settings, stats, types.InProgressStatus)); err != nil {
				b.log.Error("unable to write status", "error", err)
			}
		case <-doneCh:
			break MAIN
		}
	}

	b.log.Debugf("reporter exiting for job '%s'", settings.ID)
}

func (b *Bench) calculateWriteStats(settings *types.Settings, stats map[int]*WriteStatus, jobStatus types.JobStatus) *types.Status {
	var (
		maxElapsed      time.Duration
		maxStartedAt    time.Time
		maxEndedAt      time.Time
		numWrittenTotal int
		numErrorsTotal  int
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

		numWrittenTotal += status.NumWritten
		numErrorsTotal += status.NumErrors

		if len(status.Errors) > 0 {
			errs = append(errs, status.Errors...)
		}
	}

	// Cap of errors
	if len(errs) > 100 {
		errs = errs[:100]
	}

	return &types.Status{
		NodeID:         b.params.NodeID,
		Status:         jobStatus,
		Message:        message,
		Errors:         errs,
		JobID:          settings.ID,
		ElapsedSeconds: int(maxElapsed.Seconds()),
		AvgMsgPerSec:   int(float64(numWrittenTotal) / maxElapsed.Seconds()),
		TotalProcessed: numWrittenTotal,
		TotalErrors:    numErrorsTotal,
		StartedAt:      maxStartedAt,
		EndedAt:        maxEndedAt,
	}
}
