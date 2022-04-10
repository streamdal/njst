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

func (b *Bench) runReadBenchmark(job *types.Job) (*types.Status, error) {
	stats := map[int]*WorkerStats{}

	if job == nil || job.Settings == nil {
		return nil, errors.New("job or job settings cannot be nil")
	}

	doneCh := make(chan struct{}, 1)

	go b.runReporter(doneCh, job, stats)

	wg := &sync.WaitGroup{}

	if len(job.Settings.Read.Streams) == 0 {
		return nil, errors.New("no streams to read from")
	}

	for _, streamInfo := range job.Settings.Read.Streams {
		for i := 0; i < job.Settings.Read.NumWorkersPerStream; i++ {
			stats[i] = &WorkerStats{
				WorkerID:  i,
				StartedAt: time.Now(),
				Errors:    make([]string, 0),
			}

			wg.Add(1)

			go b.runReaderWorker(job.Context, job, i, streamInfo, stats[i], wg)
		}
	}

	// Wait for all workers to finish
	wg.Wait()

	// Stop the reporter
	doneCh <- struct{}{}

	// Calculate the final status
	return b.calculateStats(job.Settings, stats, types.CompletedStatus, "; final"), nil
}

func (b *Bench) runReaderWorker(ctx context.Context, job *types.Job, workerID int, streamInfo *types.StreamInfo, stats *WorkerStats, wg *sync.WaitGroup) {
	defer wg.Done()

	llog := b.log.WithFields(logrus.Fields{
		"worker_id": workerID,
		"stream":    streamInfo.StreamName,
		"job_id":    job.Settings.ID,
	})

	llog.Debugf("worker starting; read settings %+v", job.Settings.Read)

	sub, err := b.nats.PullSubscribe(streamInfo.StreamName, streamInfo.ConsumerGroupName)
	if err != nil {
		llog.Errorf("unable to subscribe to stream '%s': %v", streamInfo.StreamName, err)
		stats.Errors = append(stats.Errors, err.Error())
		stats.NumErrors++

		return
	}

	for {
		if stats.NumRead >= job.Settings.Read.NumMessagesPerStream {
			llog.Debugf("worker finished reading %d messages", stats.NumRead)
			break
		}

		msgs, err := sub.Fetch(job.Settings.Read.BatchSize, nats.Context(ctx))
		if err != nil {
			if strings.Contains(err.Error(), "context canceled") {
				llog.Debug("worker context cancelled - '%d' read, '%d' errors", stats.NumRead, stats.NumErrors)

				return
			}

			llog.Errorf("unable to fetch message(s): %s", err)

			stats.NumErrors++

			if stats.NumErrors > job.Settings.Read.NumMessagesPerStream {
				llog.Error("worker exiting prematurely due to too many errors")
				break
			}

			stats.Errors = append(stats.Errors, err.Error())

			continue
		}

		// Avoiding a lock here to speed things up
		stats.NumRead = stats.NumRead + len(msgs)
	}

	llog.Debug("worker exiting")

	stats.EndedAt = time.Now()
}
