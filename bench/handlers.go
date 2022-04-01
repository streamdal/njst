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
	jobName := msg.Header.Get(natssvc.HeaderJobName)
	if jobName == "" {
		b.log.Errorf("CreateMsgHandler: '%s' not found in header - skipping", natssvc.HeaderJobName)
		return
	}

	llog := b.log.WithFields(logrus.Fields{
		"func":     "CreateMsgHandler",
		"job_name": jobName,
		"node_id":  b.params.NodeID,
	})

	job := &types.Job{}

	if err := json.Unmarshal(msg.Data, job); err != nil {
		b.ReportError(jobName, fmt.Sprintf("Error unmarshalling settings: %v", err))

		return
	}

	settings := job.Settings

	llog.Debugf("starting job; settings %+v", settings)

	var status *types.Status
	var err error

	if settings.Write != nil {
		status, err = b.runWriteBenchmark(jobName, settings)
	} else if settings.Read != nil {
		status, err = b.runReadBenchmark(jobName, settings)
	} else {
		b.ReportError(jobName, "unrecognized job type - both read and write are nil")
		return
	}

	if err != nil {
		b.ReportError(jobName, fmt.Sprintf("error running benchmark: %v", err))
		return
	}

	// Send status to nats result bucket
	if err := b.nats.WriteStatus(status); err != nil {
		llog.Errorf("unable to write result status: %s", err)
	}

	llog.Debugf("job '%s' on node '%s' finished", jobName, b.params.NodeID)
}

// DeleteMsgHandler is called by natssvc when njst.$nodeID.delete is written to
func (b *Bench) DeleteMsgHandler(msg *nats.Msg) {
}

// StartMsgHandler is called by natssvc when njst.$nodeID.start is written to
func (b *Bench) StartMsgHandler(msg *nats.Msg) {
}

// StopMsgHandler is called by natssvc when njst.$nodeID.stop is written to
func (b *Bench) StopMsgHandler(msg *nats.Msg) {
}

func (b *Bench) ReportError(jobName, msg string) {
	b.log.Errorf("job: '%s' error: '%s'", jobName, msg)

	if err := b.nats.WriteStatus(&types.Status{
		JobName: jobName,
		Status:  types.ErrorStatus,
		Message: msg,
		NodeID:  b.params.NodeID,
	}); err != nil {
		b.log.Errorf("Error writing error to nats for job '%s': %v", jobName, err)
	}
}
