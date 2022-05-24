package types

import (
	"context"
	"time"
)

const (
	InProgressStatus JobStatus = "in-progress"
	ErrorStatus      JobStatus = "error"
	CompletedStatus  JobStatus = "completed"
	CancelledStatus  JobStatus = "cancelled"

	CreateJob JobType = "create"
	DeleteJob JobType = "delete"
)

type JobStatus string

type Settings struct {
	Description string         `json:"description,omitempty"`
	NATS        *NATS          `json:"nats"`
	Write       *WriteSettings `json:"write,omitempty"`
	Read        *ReadSettings  `json:"read,omitempty"`

	// Set by handler
	ID string `json:"id,omitempty"`
}

type NATS struct {
	Address          string `json:"address"`
	SharedConnection bool   `json:"shared_connection"`
}

type WriteSettings struct {
	NumStreams           int         `json:"num_streams"`
	NumNodes             int         `json:"num_nodes"`
	NumMessagesPerStream int         `json:"num_messages_per_stream"`
	NumWorkersPerStream  int         `json:"num_workers_per_stream"`
	Subjects             []string    `json:"subjects"`
	NumReplicas          int         `json:"num_replicas"`
	BatchSize            int         `json:"batch_size"`
	MsgSizeBytes         int         `json:"msg_size_bytes"`
	KeepStreams          bool        `json:"keep_streams"`
	Storage              StorageType `json:"storage"`

	// Filled out by bench.GenerateCreateJobs
	Streams []string `json:"streams,omitempty"`
}

const (
	MemoryStreamType StorageType = "memory"
	FileStorageType  StorageType = "disk"
)

type StorageType string

type ReadSettings struct {
	// WriteID should reference a completed write job
	WriteID string `json:"write_id"`

	NumStreams           int      `json:"num_streams"`
	NumNodes             int      `json:"num_nodes"`
	Nodes                []string `json:"nodes"`
	NumMessagesPerStream int      `json:"num_messages_per_stream"`
	NumWorkersPerStream  int      `json:"num_workers_per_stream"`
	BatchSize            int      `json:"batch_size"`
	Strategy             string   `json:"strategy"`

	// Filled out by bench.GenerateCreateJobs
	Streams []*StreamInfo `json:"streams,omitempty"`
}

type StreamInfo struct {
	StreamName  string
	DurableName string
}

type StatusResponse struct {
	Status   *Status   `json:"status"`
	Settings *Settings `json:"settings"`
}

type WorkerReport struct {
	WorkerID       string
	Processed      int     `json:"processed"`
	Errors         int     `json:"errors"`
	ElapsedSeconds float64 `json:"elapsed_seconds,omitempty"`
	AvgMsgPerSec   float64 `json:"avg_msg_per_sec,omitempty"` // Inf+ problem
}

type StreamReport struct {
	Workers []WorkerReport `json:"workers,omitempty"`
}

type NodeReport struct {
	Streams []StreamReport `json:"streams,omitempty"`
}

type Status struct {
	Status                 JobStatus     `json:"status"`
	Message                string        `json:"message"`
	Errors                 []string      `json:"errors,omitempty"`
	JobID                  string        `json:"job_id"`
	NodeID                 string        `json:"node_id,omitempty"`
	ElapsedSeconds         float64       `json:"elapsed_seconds,omitempty"`
	AvgMsgPerSecPerNode    float64       `json:"avg_msg_per_sec_per_node,omitempty"` // Inf+ problem
	TotalMsgPerSecAllNodes float64       `json:"total_msg_per_sec_all_nodes,omitempty"`
	AvgMsgPerSecAllNodes   float64       `json:"avg_msg_per_sec_all_nodes,omitempty"`
	TotalProcessed         int           `json:"total_processed"`
	TotalErrors            int           `json:"total_errors"`
	StartedAt              time.Time     `json:"started_at"`
	EndedAt                time.Time     `json:"ended_at,omitempty"`     // omitempty because it's not set for in-progress jobs
	NodeReport             *NodeReport   `json:"node_report,omitempty"`  // used per node
	NodeReports            []*NodeReport `json:"node_reports,omitempty"` // used for aggregate display for status
}

type PurgeRequest struct {
	All bool `json:"all"`
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
