package cli

type Params struct {
	NodeID            string   `json:"nodeID"`
	Debug             bool     `json:"debug"`
	NATSAddress       []string `json:"nats_address"`
	HTTPAddress       string   `json:"http_address"`
	NATSSubject       string   `json:"nats_subject"`
	NATSUseTLS        bool     `json:"nats_use_tls"`
	NATSTLSCaCert     string   `json:"nats_tls_ca_cert"`
	NATSTLSClientCert string   `json:"nats_tls_client_cert"`
	NATSTLSClientKey  string   `json:"nats_tls_client_key"`
	NATSTLSSkipVerify bool     `json:"nats_tls_skip_verify"`
}
