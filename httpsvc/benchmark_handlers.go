package httpsvc

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/batchcorp/njst/bench"
	"github.com/batchcorp/njst/natssvc"
	"github.com/batchcorp/njst/types"
	"github.com/julienschmidt/httprouter"
	"github.com/pkg/errors"
)

func (h *HTTPService) getBenchmarkHandler(rw http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	name := ps.ByName("name")

	if name == "" {
		writeErrorJSON(http.StatusBadRequest, "name is required", rw)
		return
	}

	status, err := h.bench.Status(name)
	if err != nil {
		writeErrorJSON(http.StatusInternalServerError, fmt.Sprintf("unable to get status: %s", err), rw)
		return
	}

	writeJSON(http.StatusOK, status, rw)
}

func (h *HTTPService) getAllBenchmarksHandler(rw http.ResponseWriter, r *http.Request) {
	// Go through results in NATS?
}

func (h *HTTPService) deleteBenchmarkHandler(rw http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	id := ps.ByName("name")

	if id == "" {
		writeErrorJSON(http.StatusBadRequest, "name is required", rw)
		return
	}

	if err := h.bench.Delete(id); err != nil {
		writeErrorJSON(http.StatusInternalServerError, fmt.Sprintf("unable to delete benchmark: %s", err), rw)
		return
	}

	rw.WriteHeader(http.StatusNoContent)
}

func (h *HTTPService) createBenchmarkHandler(rw http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		h.log.Errorf("could not read request body: %s", err)
		writeErrorJSON(http.StatusInternalServerError, fmt.Sprintf("could not read request body: %s", err), rw)
		return
	}
	defer r.Body.Close()

	settings := &types.Settings{}

	if err := json.Unmarshal(body, settings); err != nil {
		h.log.Errorf("unable to unmarshal settings: %s", err)
		writeErrorJSON(http.StatusBadRequest, fmt.Sprintf("unable to unmarshal settings: %s", err), rw)
		return
	}

	if err := validateSettings(settings); err != nil {
		h.log.Errorf("unable to validate settings: %s", err)
		writeErrorJSON(http.StatusBadRequest, fmt.Sprintf("unable to validate settings: %s", err), rw)
		return
	}

	// Does this name already exist?
	exists, err := h.bench.Exists(settings.Name)
	if err != nil {
		writeErrorJSON(http.StatusBadRequest, fmt.Sprintf("unable to verify if benchmark '%s' exists: %s", settings.Name, err), rw)
		return
	}

	if exists {
		writeErrorJSON(http.StatusBadRequest, fmt.Sprintf("benchmark '%s' already exists", settings.Name), rw)
		return
	}

	// A single node should
	jobs, err := h.bench.CreateJobs(settings)
	if err != nil {
		h.log.Errorf("unable to create streams: %s", err)
		writeErrorJSON(http.StatusInternalServerError, fmt.Sprintf("unable to create streams: %s", err), rw)
		return
	}

	// Create result bucket
	bucket, err := h.nats.GetOrCreateBucket(natssvc.ResultBucketPrefix, settings.Name)
	if err != nil {
		h.log.Errorf("unable to create result bucket: %s", err)
		writeErrorJSON(http.StatusInternalServerError, fmt.Sprintf("unable to create result bucket: %s", err), rw)
		return
	}

	bucketName := fmt.Sprintf("%s-%s", natssvc.ResultBucketPrefix, settings.Name)

	h.nats.CacheBucket(bucketName, bucket)

	if err := h.nats.EmitJobs(jobs); err != nil {
		h.log.Errorf("unable to emit jobs: %s", err)
		writeErrorJSON(http.StatusInternalServerError, fmt.Sprintf("unable to emit create benchmark: %s", err), rw)
		return
	}

	writeJSON(http.StatusOK, map[string]string{
		"message": fmt.Sprintf("benchmark '%s' created successfully", settings.Name),
	}, rw)
}

func validateSettings(settings *types.Settings) error {
	if settings == nil {
		return errors.New("settings cannot be nil")
	}

	if settings.Read == nil && settings.Write == nil {
		return errors.New("read or write settings must be set")
	}

	if settings.Name == "" {
		return errors.New("name must be set")
	}

	if !validNameRegex.MatchString(settings.Name) {
		return errors.New("name must be [a-z0-9-_]")
	}

	if settings.Read != nil {
		if err := validateReadSettings(settings.Read); err != nil {
			return err
		}
	}

	if settings.Write != nil {
		if err := validateWriteSettings(settings.Write); err != nil {
			return err
		}
	}

	return nil
}

func validateReadSettings(rs *types.ReadSettings) error {
	if rs == nil {
		return errors.New("read settings cannot be nil")
	}

	if rs.NumStreams == 0 {
		rs.NumStreams = bench.DefaultNumStreams
	}

	if rs.NumWorkers < 1 {
		rs.NumWorkers = bench.DefaultNumWorkers
	}

	if rs.BatchSize < 1 {
		rs.BatchSize = bench.DefaultBatchSize
	}

	// Valid read strategy?
	if rs.Strategy == "" {
		rs.Strategy = bench.DefaultReadStrategy
	} else {
		if _, ok := bench.ValidReadStrategies[rs.Strategy]; !ok {
			return errors.Errorf("invalid strategy '%s'", rs.Strategy)
		}
	}

	// Valid consumer group strategy?
	if rs.ConsumerGroupStrategy == "" {
		rs.ConsumerGroupStrategy = bench.DefaultConsumerGroupStrategy
	} else {
		if _, ok := bench.ValidConsumerGroupStrategies[rs.ConsumerGroupStrategy]; !ok {
			return errors.Errorf("invalid consumer group strategy '%s'", rs.ConsumerGroupStrategy)
		}
	}

	if rs.NumMessages < 1 {
		rs.NumMessages = bench.DefaultNumMessages
	}

	return nil
}

func validateWriteSettings(ws *types.WriteSettings) error {
	if ws == nil {
		return errors.New("write settings cannot be nil")
	}

	if ws.NumStreams == 0 {
		ws.NumStreams = bench.DefaultNumStreams
	}

	if ws.NumWorkers < 1 {
		ws.NumWorkers = bench.DefaultNumWorkers
	}

	if ws.MsgSizeBytes < 1 {
		ws.MsgSizeBytes = bench.DefaultMsgSizeBytes
	}

	if ws.NumMessages == 0 {
		ws.NumMessages = bench.DefaultNumMessages
	}

	if ws.NumMessages < ws.NumWorkers {
		return errors.New("num messages must be greater than or equal to num workers")
	}

	if ws.NumStreams < ws.NumWorkers {
		return errors.New("num streams must be greater than or equal to num workers")
	}

	return nil
}
