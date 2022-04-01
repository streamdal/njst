package bench

import (
	"sync"
	"time"

	"github.com/batchcorp/njst/types"
	"github.com/pkg/errors"
)

type WriteStatus struct {
	WorkerID   int
	NumWritten int
	NumErrors  int
	StartedAt  time.Time
	EndedAt    time.Time
}

func (b *Bench) runWriteBenchmark(jobName string, settings *types.Settings) (*types.Status, error) {
	stats := map[int]*WriteStatus{}

	// Generate the data
	data, err := GenRandomBytes(settings.Write.MsgSizeBytes)
	if err != nil {
		return nil, errors.Wrap(err, "unable to generate random data")
	}

	doneCh := make(chan struct{}, 1)

	go b.runReporter(doneCh, settings, stats)

	// Examples
	//
	// 4 streams, 4 workers, 1 message
	// 4 streams, 1 worker, 4 messages < ---
	// 1 stream, 4 workers, 1 message  < --- not possible caught in http handler - can't have numMessages < numWorkers
	// 1 stream, 4 workers, 4 messages <--- less annoying to just not allow having less streams than workers
	// 120 stream, 1 worker, 4 messages
	// 120 streams, 4 workers, 1 message == 120 * 4 * 1 = 480 messages ==
	numStreamsPerWorker := len(settings.Write.Subjects) / settings.Write.NumWorkers

	lower := 0
	wg := &sync.WaitGroup{}

	// Launch workers; last one gets remainder
	for i := 0; i < settings.Write.NumWorkers; i++ {
		wg.Add(1)

		upper := lower + numStreamsPerWorker
		subjects := settings.Write.Subjects[lower:upper]

		// Last worker gets remainder
		if i == settings.Write.NumWorkers-1 {
			subjects = settings.Write.Subjects[lower:]
		}

		stats[i] = &WriteStatus{
			WorkerID:  i,
			StartedAt: time.Now(),
		}

		go b.runWriterWorker(wg, i, stats[i], subjects, data, settings.Write.NumMessages)
		lower = lower + numStreamsPerWorker
	}

	// Wait for workers to finish
	wg.Wait()

	// All workers finished; signal reporter to stop
	close(doneCh)

	// Calculate the final status
	return b.calculateWriteStats(settings, stats, types.CompletedStatus), nil
}

func (b *Bench) runWriterWorker(wg *sync.WaitGroup, workerID int, stats *WriteStatus, subjects []string, data []byte, numMessages int) {
	defer wg.Done()

	for _, subj := range subjects {
		for i := 0; i < numMessages; i++ {
			err := b.nats.Publish(subj, data)
			if err != nil {
				b.log.Errorf("worker %d: unable to publish message: %s", workerID, err)
				stats.NumErrors++
				continue
			}

			stats.NumWritten++
		}
	}

	stats.EndedAt = time.Now()
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

	b.log.Debugf("reporter exiting for job '%s'", settings.Name)
}

func (b *Bench) calculateWriteStats(settings *types.Settings, stats map[int]*WriteStatus, jobStatus types.JobStatus) *types.Status {
	var maxElapsed time.Duration
	var maxStartedAt time.Time
	var numWrittenTotal int
	var numErrorsTotal int

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

		numWrittenTotal += status.NumWritten
		numErrorsTotal += status.NumErrors
	}

	return &types.Status{
		NodeID:         b.params.NodeID,
		Status:         jobStatus,
		Message:        message,
		JobName:        settings.Name,
		ElapsedSeconds: maxElapsed.Seconds(),
		AvgMsgPerSec:   float64(numWrittenTotal) / maxElapsed.Seconds(),
		TotalProcessed: numWrittenTotal,
		TotalErrors:    numErrorsTotal,
		StartedAt:      maxStartedAt,
	}
}
