package httpsvc

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/batchcorp/njst/bench"
	"github.com/batchcorp/njst/natssvc"
	"github.com/julienschmidt/httprouter"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/njst/cli"
)

type HTTPService struct {
	params  *cli.Params
	log     *logrus.Entry
	nats    *natssvc.NATSService
	bench   *bench.Bench
	version string
}

func New(params *cli.Params, n *natssvc.NATSService, b *bench.Bench, version string) (*HTTPService, error) {
	if err := validateParams(params); err != nil {
		return nil, err
	}

	if n == nil {
		return nil, errors.New("nats service cannot be nil")
	}

	if b == nil {
		return nil, errors.New("bench cannot be nil")
	}

	return &HTTPService{
		params:  params,
		log:     logrus.WithField("pkg", "httpsvc"),
		version: version,
		nats:    n,
		bench:   b,
	}, nil
}

func (h *HTTPService) Start() error {
	router := httprouter.New()

	router.HandlerFunc("GET", "/health-check", h.healthCheckHandler)
	router.HandlerFunc("GET", "/version", h.versionHandler)

	router.HandlerFunc("GET", "/bench", h.getAllBenchmarksHandler)
	router.Handle("GET", "/bench/:id", h.getBenchmarkHandler)
	router.Handle("DELETE", "/bench/:id", h.deleteBenchmarkHandler)
	router.Handle("PUT", "/bench/:id", h.updateBenchmarkHandler)
	router.HandlerFunc("POST", "/create", h.createBenchmarkHandler)

	router.HandlerFunc("GET", "/cluster", h.getClusterHandler)

	server := &http.Server{Addr: h.params.HTTPAddress, Handler: router}

	go func() {
		if err := server.ListenAndServe(); err != nil {
			h.log.Errorf("HTTP server error: %s", err)
		}
	}()

	return nil
}

func writeJSON(statusCode int, data interface{}, w http.ResponseWriter) {
	w.Header().Add("Content-type", "application/json")

	jsonData, err := json.Marshal(data)
	if err != nil {
		w.WriteHeader(500)
		logrus.Errorf("Unable to marshal data in WriteJSON: %s", err)
		return
	}

	w.WriteHeader(statusCode)

	if _, err := w.Write(jsonData); err != nil {
		logrus.Errorf("Unable to write response data: %s", err)
		return
	}
}

func writeErrorJSON(statusCode int, msg string, w http.ResponseWriter) {
	writeJSON(statusCode, map[string]string{"error": msg}, w)
}

func validateParams(params *cli.Params) error {
	if params.HTTPAddress == "" {
		return errors.New("HTTP address cannot be empty")
	}

	return nil
}
