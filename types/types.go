package types

import (
	"context"
	"time"
)

const (
	InProgressStatus JobStatus = "in-progress"
	ErrorStatus      JobStatus = "error"
	CompletedStatus  JobStatus = "completed"

	CreateJob JobType = "create"
	DeleteJob JobType = "delete"
)

type JobStatus string

type Settings struct {
	Description string         `json:"description,omitempty"`
	Write       *WriteSettings `json:"write,omitempty"`
	Read        *ReadSettings  `json:"read,omitempty"`

	// Set by handler
	ID string `json:"id,omitempty"`
}

type WriteSettings struct {
	NumStreams           int  `json:"num_streams"`
	NumNodes             int  `json:"num_nodes"`
	NumMessagesPerStream int  `json:"num_messages_per_stream"`
	NumWorkersPerStream  int  `json:"num_workers_per_stream"`
	NumReplicas          int  `json:"num_replicas"`
	MsgSizeBytes         int  `json:"msg_size_bytes"`
	KeepStreams          bool `json:"keep_streams"`

	// Filled out by bench.GenerateCreateJobs
	Subjects []string `json:"subjects,omitempty"`
}

type ReadSettings struct {
	// WriteID should reference a completed write job
	WriteID string `json:"write_id"`

	NumStreams           int `json:"num_streams"`
	NumNodes             int `json:"num_nodes"`
	NumMessagesPerStream int `json:"num_messages_per_stream"`
	NumWorkersPerStream  int `json:"num_workers_per_stream"`
	BatchSize            int `json:"batch_size"`

	// Filled out by bench.GenerateCreateJobs
	Streams []*StreamInfo `json:"streams,omitempty"`
}

type StreamInfo struct {
	StreamName        string
	ConsumerGroupName string
}

type StatusResponse struct {
	Status   *Status   `json:"status"`
	Settings *Settings `json:"settings"`
}

type Status struct {
	Status         JobStatus `json:"status"`
	Message        string    `json:"message"`
	Errors         []string  `json:"errors,omitempty"`
	JobID          string    `json:"job_id"`
	NodeID         string    `json:"node_id"`
	ElapsedSeconds int       `json:"elapsed_seconds"`
	AvgMsgPerSec   int       `json:"avg_msg_per_sec"`
	TotalProcessed int       `json:"total_processed"`
	TotalErrors    int       `json:"total_errors"`
	StartedAt      time.Time `json:"started_at"`
	EndedAt        time.Time `json:"ended_at,omitempty"` // omitempty because it's not set for in-progress jobs
}

type JobType string

type Job struct {
	NodeID   string    `json:"node_id"`
	Settings *Settings `json:"settings"`

	CreatedBy string    `json:"created_by"`
	CreatedAt time.Time `json:"created_at"`

	// Set by bench.NewJob(jobID)
	Context    context.Context    `json:"-"`
	CancelFunc context.CancelFunc `json:"-"`
}
