package bench

import (
	"context"
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
	MonitorFrequency   = 100 * time.Millisecond
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

		defer nc.Close()
	}

	for _, streamInfo := range job.Settings.Read.Streams {

		for i := 0; i < job.Settings.Read.NumWorkersPerStream; i++ {
			if workerMap[streamInfo.StreamName] == nil {
				workerMap[streamInfo.StreamName] = make(map[int]*Worker, 0)
			}

			ctx, cancel := context.WithCancel(context.Background())

			workerMap[streamInfo.StreamName][workerID] = &Worker{
				WorkerID: workerID,
				ch:       make(chan time.Time, 2),
				Errors:   make([]string, 0),
				ctx:      ctx,    // Worker specific context; read from by worker
				cancel:   cancel, // Worker specific cancel; used by monitor to signal worker to stop
			}

			wg.Add(1)

			go b.runReaderWorker(job, nc, workerID, streamInfo, workerMap[streamInfo.StreamName][workerID], wg)

			workerID++
		}
	}

	// Launch periodic workerMap aggregation & reporting
	go b.runReporter(doneCh, job, workerMap)

	// Monitor overall work and inform workers when to stop
	go b.runReaderMonitor(doneCh, job, workerMap)

	// Wait for all workers to finish
	wg.Wait()

	// Stop reporter & monitor
	close(doneCh)

	for stream := range workerMap {
		for worker := range workerMap[stream] {
			sa := <-workerMap[stream][worker].ch
			ea := <-workerMap[stream][worker].ch
			workerMap[stream][worker].StartedAt = sa
			workerMap[stream][worker].EndedAt = ea
		}
	}

	// Hack: It's possible other nodes are lagging behind this node - wait a little bit
	// before we write final status
	//time.Sleep(1 * time.Second)

	// Calculate the final status
	return b.calculateStats(job.Settings, job.NodeID, workerMap, types.CompletedStatus, "; final"), nil
}

func (b *Bench) runReaderMonitor(doneCh chan struct{}, job *types.Job, workerMap map[string]map[int]*Worker) {
	llog := b.log.WithFields(logrus.Fields{
		"method": "runReaderMonitor",
		"job":    job.Settings.ID,
	})

	ticker := time.NewTicker(MonitorFrequency)

	finishedStreams := map[string]bool{}

MAIN:
	for {
		select {
		case <-ticker.C:
			for stream, numRead := range b.calculateNumRead(workerMap) {
				// Nothing to cancel for an already finished stream
				if _, ok := finishedStreams[stream]; ok {
					continue
				}

				if numRead >= job.Settings.Read.NumMessagesPerStream {
					// Tell workgroup to stop
					for _, worker := range workerMap[stream] {
						llog.Debugf("signalling worker '%d' for stream '%s' to stop", worker.WorkerID, stream)
						go worker.cancel()
					}

					finishedStreams[stream] = true
				}
			}
		case <-doneCh:
			// Job has been completed
			llog.Debug("job completed")
			break MAIN
		case <-job.Context.Done():
			// Job has been deleted - tell all workers to exit
			llog.Debug("job asked to abort")

			for _, stream := range workerMap {
				for _, worker := range stream {
					llog.Debugf("signalling worker '%d' to stop", worker.WorkerID)
					go worker.cancel()
				}
			}

			break MAIN
		}
	}

	llog.Debug("exiting")
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

	js, err := myNC.JetStream()
	if err != nil {
		b.log.Log(logrus.ErrorLevel, "can't get JS context in ReaderWorker")
		return
	}

	sub, err := js.PullSubscribe(streamInfo.StreamName, streamInfo.DurableName)
	if err != nil {
		llog.Errorf("unable to subscribe to stream '%s': %v", streamInfo.StreamName, err)
		worker.Errors = append(worker.Errors, err.Error())
		worker.NumErrors++

		return
	}

	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			llog.Warningf("unable to unsubscribe from stream '%s': %v", streamInfo.StreamName, err)
		}
	}()

	targetNumberOfReads := job.Settings.Read.NumMessagesPerStream / (job.Settings.Read.NumWorkersPerStream * job.Settings.Read.NumNodes)

	worker.ch <- time.Now()

	for worker.NumRead < targetNumberOfReads {
		llog.Debugf("worker has read %d messages out of %d", worker.NumRead, targetNumberOfReads)

		batchSize := func() int {
			if job.Settings.Read.BatchSize <= (targetNumberOfReads - worker.NumRead) {
				return job.Settings.Read.BatchSize
			} else {
				return targetNumberOfReads - worker.NumRead
			}
		}()

		msgs, err := sub.Fetch(batchSize)
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
			if err := msg.Ack(); err != nil {
				llog.Warningf("unable to ack message: %s", err)
			}
		}
	}
	worker.ch <- time.Now()

	llog.Debugf("worker exiting; '%d' read, '%d' errors", worker.NumRead, worker.NumErrors)
}
