package types

import (
	"time"
)

// -------------------------------------------------------------------------- //
// 							Settings passed via HTTP API 	        		  //
// -------------------------------------------------------------------------- //

type Settings struct {
	Name                 string            `json:"name,omitempty"`
	Description          string            `json:"description,omitempty"`
	NumStreams           int               `json:"num_streams"`
	NumReplicas          int               `json:"num_replicas"`
	NumMessagesPerStream int               `json:"num_messages_per_stream"`
	Producer             *ProducerSettings `json:"producer"`
	Consumer             *ConsumerSettings `json:"consumer"`

	// Set in create API handler
	BenchID string
}

type ProducerSettings struct {
	MsgSizeBytes int `json:"msg_size_bytes"`
}

type ConsumerSettings struct {
	BatchSize int `json:"batch_size"`
}

// -------------------------------------------------------------------------- //
//               Settings passed via NATS for njst instances                  //
// -------------------------------------------------------------------------- //

type NATSSettings struct {
	BenchID  string                `json:"bench_id,omitempty"` // Ignored on create request
	NodeID   string                `json:"node_id"`
	Producer *ProducerNATSSettings `json:"producer"`
	Consumer *ConsumerNATSSettings `json:"consumer"`
}

type ProducerNATSSettings struct {
	StreamID         string `json:"stream_id"` // Overwritten at create
	Subject          string `json:"subject"`   // Overwritten at create
	MsgSizeBytes     int    `json:"msg_size_bytes"`
	NumMsgsPerStream int    `json:"num_msgs_per_stream"`
}

type ConsumerNATSSettings struct {
	StreamID          string `json:"stream_id"`           // Overwritten at create
	ConsumerGroupName string `json:"consumer_group_name"` // Overwritten at create
	BatchSize         int    `json:"batch_size"`
	NumMsgsPerStream  int    `json:"num_msgs_per_stream"`
	StartDelaySeconds int    `json:"start_delay_seconds"`
}

// -------------------------------------------------------------------------- //
//                                 Other                                      //
// -------------------------------------------------------------------------- //

type JobStatus string

type Status struct {
	Status                  JobStatus        `json:"status"`
	ProducerElapsedDuration time.Duration    `json:"producer_elapsed_duration,omitempty"`
	ConsumerElapsedDuration time.Duration    `json:"consumer_elapsed_duration,omitempty"`
	ProducerAvgMsgPerSec    float64          `json:"producer_avg_msg_per_sec,omitempty"`
	ConsumerAvgMsgPerSec    float64          `json:"consumer_avg_msg_per_sec,omitempty"`
	TotalProduced           int              `json:"total_produced,omitempty"`
	TotalConsumed           int              `json:"total_consumed,omitempty"`
	StartedAt               time.Time        `json:"started_at"`
	EndedAt                 time.Time        `json:"ended_at"`
	Producer                []ProducerStatus `json:"producer,omitempty"`
	Consumer                []ConsumerStatus `json:"consumer,omitempty"`
}

type ProducerStatus struct {
	NodeID          string        `json:"node_id"`
	Status          JobStatus     `json:"status"`
	NumWritten      int           `json:"num_written"`
	ElapsedDuration time.Duration `json:"elapsed_duration"`
	AvgMsgPerSec    float64       `json:"avg_msg_per_sec"`
}

type ConsumerStatus struct {
	NodeID          string        `json:"node_id"`
	Status          JobStatus     `json:"status"`
	NumRead         int           `json:"num_read"`
	ElapsedDuration time.Duration `json:"elapsed_duration"`
	AvgMsgPerSec    float64       `json:"avg_msg_per_sec"`
}
