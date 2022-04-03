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

	job := &types.Job{}

	if err := json.Unmarshal(msg.Data, job); err != nil {
		b.ReportError(jobID, fmt.Sprintf("Error unmarshalling settings: %v", err))

		return
	}

	settings := job.Settings

	llog.Debugf("starting job; settings %+v", settings)

	var status *types.Status
	var err error

	if settings.Write != nil {
		status, err = b.runWriteBenchmark(settings)
	} else if settings.Read != nil {
		status, err = b.runReadBenchmark(settings)
	} else {
		b.ReportError(jobID, "unrecognized job type - both read and write are nil")
		return
	}

	if err != nil {
		b.ReportError(jobID, fmt.Sprintf("error running benchmark: %v", err))
		return
	}

	// Send final status to nats result bucket
	if err := b.nats.WriteStatus(status); err != nil {
		llog.Errorf("unable to write result status: %s", err)
	}

	llog.Debugf("job '%s' on node '%s' finished", jobID, b.params.NodeID)
}

// DeleteMsgHandler is called by natssvc when njst.$nodeID.delete is written to
func (b *Bench) DeleteMsgHandler(msg *nats.Msg) {
	jobID := msg.Header.Get(natssvc.HeaderJobID)

	if jobID == "" {
		b.log.Errorf("DeleteMsgHandler: '%s' not found in header - skipping", natssvc.HeaderJobID)
		return
	}

	llog := b.log.WithFields(logrus.Fields{
		"func":    "CreateMsgHandler",
		"job_id":  jobID,
		"node_id": b.params.NodeID,
	})

	// We don't need to unmarshal the job since we already have the id
	llog.Debugf("deleting job '%s' on node '%s'", jobID, b.params.NodeID)

	// Is this job running?
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
