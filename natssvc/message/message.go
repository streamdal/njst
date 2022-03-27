package message

type CreateBenchmarkRequest struct {
	BenchmarkID string `json:"benchmark_id"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

type DeleteBenchmarkRequest struct {
	BenchmarkID string `json:"benchmark_id"`

	// Debug info
	RequestedAtUTCNano int64  `json:"requested_at_utc_nano"`
	RequestedBy        string `json:"requested_by"`
}
