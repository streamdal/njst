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

	wg := &sync.WaitGroup{}

	if len(job.Settings.Read.Streams) == 0 {
		return nil, errors.New("no streams to read from")
	}

	msgsPerWorker := job.Settings.Read.NumMessagesPerStream / job.Settings.Read.NumWorkersPerStream
	msgsPerWorkerRemainder := job.Settings.Read.NumMessagesPerStream % job.Settings.Read.NumWorkersPerStream

	var workerID int

	for _, streamInfo := range job.Settings.Read.Streams {
		for i := 0; i < job.Settings.Read.NumWorkersPerStream; i++ {
			stats[workerID] = &WorkerStats{
				WorkerID:  workerID,
				StartedAt: time.Now(),
				Errors:    make([]string, 0),
			}

			wg.Add(1)

			numMessages := msgsPerWorker

			// Last worker gets the remainder of messages
			if i == job.Settings.Read.NumWorkersPerStream-1 {
				numMessages = msgsPerWorker + msgsPerWorkerRemainder
			}

			go b.runReaderWorker(job.Context, job, workerID, numMessages, streamInfo, stats[workerID], wg)

			workerID++
		}
	}

	// Launch a reporter
	go b.runReporter(doneCh, job, stats)

	// Wait for all workers to finish
	wg.Wait()

	// Stop the reporter
	doneCh <- struct{}{}

	// Calculate the final status
	return b.calculateStats(job.Settings, stats, types.CompletedStatus, "; final"), nil
}

func (b *Bench) runReaderWorker(ctx context.Context, job *types.Job, workerID int, numMessages int, streamInfo *types.StreamInfo, stats *WorkerStats, wg *sync.WaitGroup) {
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
		if stats.NumRead >= numMessages {
			llog.Debugf("worker finished reading %d messages", stats.NumRead)
			break
		}

		llog.Debugf("worker has read %d messages", stats.NumRead)

		msgs, err := sub.Fetch(job.Settings.Read.BatchSize, nats.MaxWait(5*time.Second))
		if err != nil {
			llog.Debugf("got an error; number of messages: %d", len(msgs))

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

		for _, msg := range msgs {
			if err := msg.Ack(); err != nil {
				llog.Warningf("unable to ack message: %s", err)
			}

			stats.NumRead++
		}

		//stats.NumRead += len(msgs)
	}

	llog.Debugf("worker exiting; read '%d' messages", stats.NumRead)

	stats.EndedAt = time.Now()
}
