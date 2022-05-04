package bench

import (
	"context"
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

	numMessagesPerWorker := job.Settings.Write.NumMessagesPerStream / (job.Settings.Write.NumNodes * job.Settings.Write.NumWorkersPerStream)
	numMessagesPerLastWorker := numMessagesPerWorker + (job.Settings.Write.NumMessagesPerStream % job.Settings.Write.NumWorkersPerStream)
	var nc *nats.Conn

	if job.Settings.NATS.SharedConnection {
		nc, err = b.nats.NewConn(job.Settings.NATS)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to create nats connection for job %s", job.Settings.ID)
		}

		defer nc.Drain()
	}
	// Launch workers; last one gets remainder
	for _, subj := range job.Settings.Write.Subjects {
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
	return b.calculateStats(job.Settings, job.NodeID, workerMap, types.CompletedStatus, "; final"), nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (b *Bench) runWriterWorker(ctx context.Context, nc *nats.Conn, job *types.Job, workerID int, subj string, data []byte, numMessages int, worker *Worker, wg *sync.WaitGroup) {
	var batchSize = job.Settings.Write.BatchSize
	if batchSize == 0 {
		batchSize = 100
	} // TODO find out why the batch size doesn't set to default in WriteSettings like it does in ReadSettings

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
	js, err := myNC.JetStream()
	if err != nil {
		b.log.Log(logrus.ErrorLevel, "can't get JS context in WriteWorker")
		return
	}

	llog := b.log.WithFields(logrus.Fields{
		"worker_id":   workerID,
		"subject":     subj,
		"numMessages": numMessages,
	})

	llog.Debug("worker starting")
	worker.ch <- time.Now()

	for i := 0; i < numMessages; i += batchSize {
		futures := make([]nats.PubAckFuture, min(batchSize, numMessages-i))
		for j := 0; j < batchSize && i+j < numMessages; j++ {
			futures[j], err = js.PublishAsync(subj, data)
			if err != nil {
				llog.Errorf("unable to JS async publish message: %s", err)
				worker.NumErrors++

				if worker.NumErrors > numMessages {
					llog.Error("worker exiting prematurely due to too many errors")
					break
				}
				worker.Errors = append(worker.Errors, err.Error())
				continue
			}
		}
		select {
		case <-js.PublishAsyncComplete():
			for future := range futures {
				select {
				case <-futures[future].Ok():
					worker.NumWritten++
				case e := <-futures[future].Err():
					llog.Error("PubAsyncFuture for message %v in batch not OK: %v", future, e)
					worker.NumErrors++

					if worker.NumErrors > numMessages {
						llog.Error("worker exiting prematurely due to too many errors")
						break
					}

					worker.Errors = append(worker.Errors, err.Error())
				}
			}
		case <-time.After(5 * time.Second):
			llog.Error("PublishAsyncComplete timed out after 5s")
		}
	}

	worker.ch <- time.Now()
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

	if jobStatus == types.CompletedStatus {
		message = "benchmark completed"
	}

	var streamReports []types.StreamReport
	for i, stream := range workerMap {
		workerReports := make([]types.WorkerReport, len(workerMap[i]))
		var workerTotalElapsed time.Duration = 0 * time.Second
		for j, worker := range stream {
			report := types.WorkerReport{WorkerID: fmt.Sprintf("%s-%s-%d", nodeId, i, worker.WorkerID)}
			workerElapsed := worker.EndedAt.Sub(worker.StartedAt)
			report.ElapsedSeconds = workerElapsed.Seconds()
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
			report.AvgMsgPerSec = float64(workerNumProcessed) / workerElapsed.Seconds()
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
			workerReports[j] = report
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
		NodeReport: types.NodeReport{
			Streams: streamReports,
		},
	}
}
