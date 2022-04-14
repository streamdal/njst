package httpsvc

import (
	"encoding/json"
	"math/rand"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/batchcorp/njst/bench"
	"github.com/batchcorp/njst/natssvc"
	"github.com/julienschmidt/httprouter"
	"github.com/pkg/errors"
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

var (
	runes = []rune("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
)

func init() {
	rand.Seed(time.Now().UnixNano())
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

	if h.params.EnablePprof {
		// pprof stuff
		router.HandlerFunc(http.MethodGet, "/debug/pprof/", pprof.Index)
		router.HandlerFunc(http.MethodGet, "/debug/pprof/cmdline", pprof.Cmdline)
		router.HandlerFunc(http.MethodGet, "/debug/pprof/profile", pprof.Profile)
		router.HandlerFunc(http.MethodGet, "/debug/pprof/symbol", pprof.Symbol)
		router.HandlerFunc(http.MethodGet, "/debug/pprof/trace", pprof.Trace)
		router.Handler(http.MethodGet, "/debug/pprof/goroutine", pprof.Handler("goroutine"))
		router.Handler(http.MethodGet, "/debug/pprof/heap", pprof.Handler("heap"))
		router.Handler(http.MethodGet, "/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
		router.Handler(http.MethodGet, "/debug/pprof/block", pprof.Handler("block"))
	}

	router.HandlerFunc("GET", "/health-check", h.healthCheckHandler)
	router.HandlerFunc("GET", "/version", h.versionHandler)

	router.HandlerFunc("GET", "/bench", h.getAllBenchmarksHandler)
	router.Handle("GET", "/bench/:id", h.getBenchmarkHandler)
	router.Handle("DELETE", "/bench/:id", h.deleteBenchmarkHandler)
	router.HandlerFunc("POST", "/bench", h.createBenchmarkHandler)

	router.HandlerFunc("GET", "/cluster", h.getClusterHandler)

	server := &http.Server{Addr: h.params.HTTPAddress, Handler: router}

	errCh := make(chan error, 0)

	go func() {
		if err := server.ListenAndServe(); err != nil {
			errCh <- err
		}
	}()

	timer := time.NewTimer(5 * time.Second)

	select {
	case err := <-errCh:
		return errors.Wrap(err, "unable to listen and serve")
	case <-timer.C:
		// No error after 5s
		return nil
	}
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

func RandString(n int) string {
	b := make([]rune, n)

	for i := range b {
		b[i] = runes[rand.Intn(len(runes))]
	}

	return string(b)
}
