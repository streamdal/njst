package bench

import (
	"fmt"
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

	// If there are multiple subjects, each worker writes a portion of NumMessages
	// to each subject
	numMessagesPerWorker := job.Settings.Write.NumMessagesPerStream / (job.Settings.Write.NumNodes * job.Settings.Write.NumWorkersPerStream)
	numMessagesPerLastWorker := numMessagesPerWorker + (job.Settings.Write.NumMessagesPerStream % job.Settings.Write.NumWorkersPerStream)

	numMessagesPerWorkerPerSubject := numMessagesPerWorker / job.Settings.Write.NumSubjectsPerStream
	numMessagesPerLastWorkerPerSubject := numMessagesPerWorkerPerSubject + (numMessagesPerLastWorker % job.Settings.Write.NumSubjectsPerStream)

	var nc *nats.Conn

	if job.Settings.NATS.SharedConnection {
		nc, err = b.nats.NewConn(job.Settings.NATS)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to create nats connection for job %s", job.Settings.ID)
		}

		defer nc.Drain()
	}

	// Launch workers; last one gets remainder
	for _, stream := range job.Settings.Write.Streams {
		for i := 0; i < job.Settings.Write.NumWorkersPerStream; i++ {
			if _, ok := workerMap[stream]; !ok {
				workerMap[stream] = make(map[int]*Worker, 0)
			}

			workerMap[stream][i] = &Worker{
				WorkerID: i,
				Errors:   make([]string, 0),
			}

			wg.Add(1)

			// Last worker gets remaining messages
			if i == job.Settings.Write.NumWorkersPerStream-1 {
				go b.runWriterWorker(nc, job, i, stream, data, numMessagesPerLastWorkerPerSubject, workerMap[stream][i], wg)
			} else {
				go b.runWriterWorker(nc, job, i, stream, data, numMessagesPerWorkerPerSubject, workerMap[stream][i], wg)
			}
		}
	}

	// Wait for all workers to finish
	wg.Wait()

	// Stop the reporter
	doneCh <- struct{}{}

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

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (b *Bench) runWriterWorker(nc *nats.Conn, job *types.Job, workerID int, stream string, data []byte, numMessages int, worker *Worker, wg *sync.WaitGroup) {
	var batchSize = job.Settings.Write.BatchSize

	if batchSize == 0 {
		batchSize = 100
	}

	var myNC = nc
	var err error

	defer wg.Done()

	if !job.Settings.NATS.SharedConnection {
		myNC, err = b.nats.NewConn(job.Settings.NATS)
		if err != nil {
			b.log.Log(logrus.ErrorLevel, "can't get connection for connection per worker: ", err)
			return
		}

		defer myNC.Drain()
	} else if nc == nil {
		b.log.Error("worker not passed a valid NATS Connection and connection per worker specified")
		return
	}

	//js, err := nc.JetStream(nats.PublishAsyncMaxPending(batchSize))
	// No need to rely on the context Max pub async setting for flow control as now checking the puback futures in batches
	js, err := myNC.JetStream(nats.Context(job.Context))
	if err != nil {
		b.log.Log(logrus.ErrorLevel, "can't get JS context in WriteWorker")
		return
	}

	llog := b.log.WithFields(logrus.Fields{
		"worker_id":   workerID,
		"stream":      stream,
		"numMessages": numMessages,
	})

	llog.Debug("worker starting")

	// Record started at time
	worker.StartedAt = time.Now().UTC()

MAIN:
	for s := 0; s < job.Settings.Write.NumSubjectsPerStream; s++ {
		for i := 0; i < numMessages; i += batchSize {
			futures := make([]nats.PubAckFuture, min(batchSize, numMessages-i))

			for j := 0; j < batchSize && i+j < numMessages; j++ {
				futures[j], err = js.PublishAsync(fmt.Sprintf("%s.%d", stream, s), data)
				if err != nil {
					llog.Errorf("unable to JS async publish message: %s", err)
					worker.NumErrors++
					worker.Errors = append(worker.Errors, err.Error())

					if worker.NumErrors > numMessages {
						llog.Error("worker exiting prematurely due to too many errors")
						break MAIN
					}

					continue
				}
			}

			select {
			case <-job.Context.Done():
				llog.Debug("worker exiting due to context done")
				return
			case <-js.PublishAsyncComplete():
				for future := range futures {
					select {
					case <-futures[future].Ok():
						worker.NumWritten++
					case e := <-futures[future].Err():
						llog.Errorf("PubAsyncFuture for message %v in batch not OK: %v", future, e)

						worker.NumErrors++
						worker.Errors = append(worker.Errors, e.Error())

						if worker.NumErrors > numMessages {
							llog.Error("worker exiting prematurely due to too many errors")
							break MAIN
						}
					}
				}
			case <-time.After(10 * time.Second):
				llog.Error("PublishAsyncComplete timed out after 10s")

				worker.NumErrors++
				worker.Errors = append(worker.Errors, fmt.Sprintf(
					"PublishAsyncComplete timed out after 10s (pending: %d)", js.PublishAsyncPending()))

				if worker.NumErrors > numMessages {
					llog.Error("worker exiting prematurely due to too many errors")
					break MAIN
				}
			}
		}
	}

	// Record ended at
	worker.EndedAt = time.Now().UTC()

	llog.Debugf("worker exiting; wrote '%d' messages", worker.NumWritten)

}

func (b *Bench) calculateStats(settings *types.Settings, nodeId string, workerMap map[string]map[int]*Worker, jobStatus types.JobStatus, msg string) *types.Status {
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

	switch jobStatus {
	case types.CompletedStatus:
		message = "benchmark completed"
	case types.CancelledStatus:
		message = "benchmark cancelled"
	case types.InProgressStatus:
		message = "benchmark is in progress"
	}

	var streamReports []types.StreamReport
	for i, stream := range workerMap {
		workerReports := make([]types.WorkerReport, 0)
		workerTotalElapsed := 0 * time.Second

		for _, worker := range stream {
			report := types.WorkerReport{
				WorkerID: fmt.Sprintf("%s-%s-%d", nodeId, i, worker.WorkerID),
			}

			var workerElapsed time.Duration

			if worker.EndedAt.IsZero() {
				workerElapsed = time.Now().UTC().Sub(worker.StartedAt)
			} else {
				workerElapsed = worker.EndedAt.Sub(worker.StartedAt)
			}

			report.ElapsedSeconds = round(workerElapsed.Seconds(), 2)
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

			report.Processed = workerNumProcessed
			numProcessedTotal += workerNumProcessed
			report.AvgMsgPerSec = round(float64(workerNumProcessed)/workerElapsed.Seconds(), 2)

			totalPerWorkGroupAverages += report.AvgMsgPerSec

			report.Errors = worker.NumErrors
			numErrorsTotal += worker.NumErrors
			if len(worker.Errors) > 0 {
				errs = append(errs, worker.Errors...)
			}

			if minStartedAt.IsZero() {
				minStartedAt = worker.StartedAt
			}

			if worker.StartedAt.Before(minStartedAt) {
				minStartedAt = worker.StartedAt
			}

			if worker.EndedAt.After(maxEndedAt) {
				maxEndedAt = worker.EndedAt
			}

			workerReports = append(workerReports, report)
		}

		totalElapsed += workerTotalElapsed
		streamReports = append(streamReports, types.StreamReport{Workers: workerReports})
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
		NodeReport: &types.NodeReport{
			Streams: streamReports,
		},
	}
}
