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
	MonitorFrequency   = time.Second
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
	var workerID int

	for _, streamInfo := range job.Settings.Read.Streams {
		for i := 0; i < job.Settings.Read.NumWorkersPerStream; i++ {
			if workerMap[streamInfo.StreamName] == nil {
				workerMap[streamInfo.StreamName] = make(map[int]*Worker, 0)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

			workerMap[streamInfo.StreamName][workerID] = &Worker{
				WorkerID:  workerID,
				StartedAt: time.Now(),
				Errors:    make([]string, 0),
				ctx:       ctx,    // Worker specific context; read from by worker
				cancel:    cancel, // Worker specific cancel; used by monitor to signal worker to stop
			}

			wg.Add(1)

			go b.runReaderWorker(job, workerID, streamInfo, workerMap[streamInfo.StreamName][workerID], wg)

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

	// Hack: It's possible other nodes are lagging behind this node - wait a little bit
	// before we write final status
	time.Sleep(5 * time.Second)

	// Calculate the final status
	return b.calculateStats(job.Settings, workerMap, types.CompletedStatus, "; final"), nil
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

func (b *Bench) runReaderWorker(job *types.Job, workerID int, streamInfo *types.StreamInfo, worker *Worker, wg *sync.WaitGroup) {
	defer func() {
		worker.EndedAt = time.Now()
		wg.Done()
	}()

	llog := b.log.WithFields(logrus.Fields{
		"worker_id": workerID,
		"stream":    streamInfo.StreamName,
		"job_id":    job.Settings.ID,
	})

	llog.Debugf("worker starting; read settings %+v", job.Settings.Read)

	//conn, err := b.nats.NewConn()
	//if err != nil {
	//	llog.Errorf("error creating nats connection: %s", err)
	//	return
	//}
	//
	//js, err := conn.JetStream()
	//if err != nil {
	//	llog.Errorf("error creating jetstream context: %s", err)
	//	return
	//}

	sub, err := b.nats.PullSubscribe(streamInfo.StreamName, streamInfo.ConsumerGroupName)
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

	for {
		llog.Debugf("worker has read %d messages", worker.NumRead)

		msgs, err := sub.Fetch(job.Settings.Read.BatchSize, nats.Context(worker.ctx))
		if err != nil {
			if strings.Contains(err.Error(), "context canceled") {
				llog.Debug("worker asked to exit")

				break
			}

			llog.Errorf("unable to fetch message(s): %s", err)

			worker.NumErrors++

			if worker.NumErrors > MaxErrorsPerWorker {
				llog.Error("worker exiting prematurely due to too many errors")

				break
			}

			worker.Errors = append(worker.Errors, err.Error())

			continue
		}

		for _, msg := range msgs {
			if err := msg.Ack(); err != nil {
				llog.Warningf("unable to ack message: %s", err)
			}

			worker.NumRead++
		}
	}

	llog.Debugf("worker exiting; '%d' read, '%d' errors", worker.NumRead, worker.NumErrors)
}
