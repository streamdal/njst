package types

import (
	"time"
)

const (
	SpreadReadStrategy ReadStrategy = "spread"
	SharedReadStrategy ReadStrategy = "shared"

	NoneConsumerGroupStrategy      ConsumerGroupStrategy = "none"
	PerStreamConsumerGroupStrategy ConsumerGroupStrategy = "per_stream"
	PerJobConsumerGroupStrategy    ConsumerGroupStrategy = "per_job"

	InProgressStatus JobStatus = "in-progress"
	ErrorStatus      JobStatus = "error"
	CompletedStatus  JobStatus = "completed"

	CreateJob JobType = "create"
	DeleteJob JobType = "delete"
)

type ReadStrategy string
type ConsumerGroupStrategy string
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
	NumStreams           int `json:"num_streams"`
	NumNodes             int `json:"num_nodes"`
	NumMessagesPerStream int `json:"num_messages"`
	NumWorkersPerStream  int `json:"num_workers"`
	BatchSize            int `json:"batch_size"`

	Strategy              ReadStrategy          `json:"strategy"`
	ConsumerGroupStrategy ConsumerGroupStrategy `json:"consumer_group_strategy"`

	// Filled out by bench.GenerateCreateJobs
	Streams []string `json:"streams"`
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
}
