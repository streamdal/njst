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
)

type ReadStrategy string
type ConsumerGroupStrategy string
type JobStatus string

type Settings struct {
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	Write       *WriteSettings `json:"write"`
	Read        *ReadSettings  `json:"read"`
}

type WriteSettings struct {
	NumStreams   int  `json:"num_streams"`
	NumMessages  int  `json:"num_messages"`
	NumNodes     int  `json:"num_nodes"`
	NumWorkers   int  `json:"num_workers"`
	NumReplicas  int  `json:"num_replicas"`
	MsgSizeBytes int  `json:"msg_size_bytes"`
	KeepStreams  bool `json:"keep_streams"`

	// Filled out by bench.CreateJobs
	Subjects []string `json:"subjects"`
}

type ReadSettings struct {
	NumStreams  int `json:"num_streams"`
	NumMessages int `json:"num_messages"`
	NumNodes    int `json:"num_nodes"`
	NumWorkers  int `json:"num_workers"`
	BatchSize   int `json:"batch_size"`

	Strategy              ReadStrategy          `json:"strategy"`
	ConsumerGroupStrategy ConsumerGroupStrategy `json:"consumer_group_strategy"`

	// Filled out by bench.CreateJobs
	Streams []string `json:"streams"`
}

type Status struct {
	Status         JobStatus `json:"status"`
	Message        string    `json:"message,omitempty"`
	JobName        string    `json:"job_name"`
	NodeID         string    `json:"node_id"`
	ElapsedSeconds float64   `json:"elapsed_seconds,omitempty"`
	AvgMsgPerSec   float64   `json:"avg_msg_per_sec,omitempty"`
	TotalProcessed int       `json:"total_processed,omitempty"`
	TotalErrors    int       `json:"total_errors,omitempty"`
	StartedAt      time.Time `json:"started_at"`
	EndedAt        time.Time `json:"ended_at"`
}

// ----------------------------------------------------------------------------

type Job struct {
	NodeID   string    `json:"node_id"`
	Settings *Settings `json:"settings"`

	CreatedBy string    `json:"created_by"`
	CreatedAt time.Time `json:"created_at"`
}
