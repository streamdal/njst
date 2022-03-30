package types

import (
	"time"
)

const (
	SingleWriteStrategy WriteStrategy = "single"
	SpreadWriteStrategy WriteStrategy = "spread"

	SpreadReadStrategy ReadStrategy = "spread"
	SharedReadStrategy ReadStrategy = "shared"
)

type Settings struct {
	Name        string         `json:"name,omitempty"`
	Description string         `json:"description,omitempty"`
	NumStreams  int            `json:"num_streams"`
	NumReplicas int            `json:"num_replicas"`
	NumMessages int            `json:"num_messages"`
	NumNodes    int            `json:"num_nodes"`
	NumWorkers  int            `json:"num_workers"`
	Write       *WriteSettings `json:"write"`
	Read        *ReadSettings  `json:"read"`
}

type WriteSettings struct {
	Strategy     WriteStrategy `json:"strategy"`
	MsgSizeBytes int           `json:"msg_size_bytes"`
}

type ReadSettings struct {
	Strategy              ReadStrategy          `json:"strategy"`
	BatchSize             int                   `json:"batch_size"`
	ConsumerGroupStrategy ConsumerGroupStrategy `json:"consumer_group_strategy"`
}

type ReadStrategy string
type WriteStrategy string
type ConsumerGroupStrategy string
type JobStatus string

type Status struct {
	Status          JobStatus     `json:"status"`
	ElapsedDuration time.Duration `json:"elapsed_duration,omitempty"`
	AvgMsgPerSec    float64       `json:"avg_msg_per_sec,omitempty"`
	TotalProcessed  int           `json:"total_processed,omitempty"`
	StartedAt       time.Time     `json:"started_at"`
	EndedAt         time.Time     `json:"ended_at"`
}
