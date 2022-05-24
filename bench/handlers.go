package bench

import (
	"encoding/json"
	"fmt"

	"github.com/batchcorp/njst/natssvc"
	"github.com/batchcorp/njst/types"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
)

// CreateMsgHandler is called by natssvc when njst.$nodeID.create is written to
func (b *Bench) CreateMsgHandler(msg *nats.Msg) {
	jobID := msg.Header.Get(natssvc.HeaderJobID)

	if jobID == "" {
		b.log.Errorf("CreateMsgHandler: '%s' not found in header - skipping", natssvc.HeaderJobID)
		return
	}

	llog := b.log.WithFields(logrus.Fields{
		"func":    "CreateMsgHandler",
		"job_id":  jobID,
		"node_id": b.params.NodeID,
	})

	llog.Debugf("Data contents: %v", string(msg.Data))

	llog.Info("Received new create job")

	job := b.newJob(jobID)
	defer b.deleteJob(jobID)

	if err := json.Unmarshal(msg.Data, job); err != nil {
		b.ReportError(jobID, fmt.Sprintf("Error unmarshalling settings: %v", err))

		return
	}

	llog.Debugf("starting job; write settings %+v; read settings %+v", job.Settings.Write, job.Settings.Read)

	var status *types.Status
	var err error

	if job.Settings.Write != nil {
		llog.Info("Performing write job")
		status, err = b.runWriteBenchmark(job)
	} else if job.Settings.Read != nil {
		llog.Info("Performing read job")
		status, err = b.runReadBenchmark(job)
	} else {
		b.ReportError(jobID, "unrecognized job type - both read and write are nil")
		return
	}

	if err != nil {
		b.ReportError(jobID, fmt.Sprintf("error running benchmark: %v", err))
		return
	}

	if err := b.nats.WriteStatus(status); err != nil {
		llog.Debugf("Unable to write final result status: %s", err)
	}

	llog.Info("Job complete")
}

// DeleteMsgHandler is called by natssvc when njst.$nodeID.delete is written to
func (b *Bench) DeleteMsgHandler(msg *nats.Msg) {
	jobID := msg.Header.Get(natssvc.HeaderJobID)

	if jobID == "" {
		b.log.Errorf("DeleteMsgHandler: '%s' not found in header - skipping", natssvc.HeaderJobID)
		return
	}

	llog := b.log.WithFields(logrus.Fields{
		"func":    "DeleteMsgHandler",
		"job_id":  jobID,
		"node_id": b.params.NodeID,
	})

	// Is this job running?
	job, ok := b.getJob(jobID)
	if !ok {
		b.log.Debugf("job '%s' not found on node '%s' - nothing to do", jobID, b.params.NodeID)
		return
	}

	llog.Info("Received new delete job")

	// Cancel running reporter + worker(s)
	job.CancelFunc()

	// Delete job from mem
	b.deleteJob(jobID)

	b.log.Info("Job deleted")
}

func (b *Bench) ReportError(jobID, msg string) {
	b.log.Errorf("job: '%s' error: '%s'", jobID, msg)

	if err := b.nats.WriteStatus(&types.Status{
		JobID:   jobID,
		Status:  types.ErrorStatus,
		Message: msg,
		NodeID:  b.params.NodeID,
	}); err != nil {
		b.log.Errorf("Error writing error to nats for job '%s': %v", jobID, err)
	}
}
