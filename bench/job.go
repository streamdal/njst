package bench

import (
	"context"

	"github.com/batchcorp/njst/types"
)

func (b *Bench) newJob(jobID string) *types.Job {
	ctx, cancel := context.WithCancel(context.Background())

	job := &types.Job{
		Context:    ctx,
		CancelFunc: cancel,
	}

	b.jobsMutex.Lock()
	defer b.jobsMutex.Unlock()

	b.jobs[jobID] = job

	return job
}

func (b *Bench) getJob(jobID string) (*types.Job, bool) {
	b.jobsMutex.RLock()
	defer b.jobsMutex.RUnlock()

	job, ok := b.jobs[jobID]

	return job, ok
}

// Called from nats handler
func (b *Bench) deleteJob(id string) {
	b.jobsMutex.Lock()
	defer b.jobsMutex.Unlock()

	delete(b.jobs, id)
}
