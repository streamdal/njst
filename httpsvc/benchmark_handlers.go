package httpsvc

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/batchcorp/njst/bench"
	"github.com/batchcorp/njst/natssvc"
	"github.com/batchcorp/njst/types"
	"github.com/julienschmidt/httprouter"
	"github.com/pkg/errors"
)

func (h *HTTPService) getBenchmarkHandler(rw http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	id := ps.ByName("id")

	if id == "" {
		writeErrorJSON(http.StatusBadRequest, "id is required", rw)
		return
	}

	status, err := h.bench.Status(id)
	if err != nil {
		if strings.Contains(err.Error(), "unable to get") {
			writeErrorJSON(http.StatusNotFound, err.Error(), rw)
			return
		}

		writeErrorJSON(http.StatusInternalServerError, fmt.Sprintf("unable to get status: %s", err), rw)
		return
	}

	settings, err := h.nats.GetSettings(id)
	if err != nil {
		writeErrorJSON(http.StatusInternalServerError, fmt.Sprintf("unable to get settings: %s", err), rw)
		return
	}

	writeJSON(http.StatusOK, &types.StatusResponse{
		Status:   status,
		Settings: settings,
	}, rw)
}

func (h *HTTPService) getAllBenchmarksHandler(rw http.ResponseWriter, r *http.Request) {
	allSettings, err := h.nats.GetAllSettings()
	if err != nil {
		writeErrorJSON(http.StatusInternalServerError, fmt.Sprintf("unable to get settings: %s", err), rw)
		return
	}

	writeJSON(http.StatusOK, allSettings, rw)
}

func (h *HTTPService) deleteBenchmarkHandler(rw http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	id := ps.ByName("id")

	if id == "" {
		writeErrorJSON(http.StatusBadRequest, "id is required", rw)
		return
	}

	var (
		deleteStreams  bool
		deleteSettings bool
		deleteResults  bool
	)

	if _, ok := r.URL.Query()["streams"]; ok {
		deleteStreams = true
	}

	if _, ok := r.URL.Query()["settings"]; ok {
		deleteSettings = true
	}

	if _, ok := r.URL.Query()["results"]; ok {
		deleteResults = true
	}

	if err := h.bench.Delete(id, deleteStreams, deleteSettings, deleteResults); err != nil {
		writeErrorJSON(http.StatusInternalServerError, fmt.Sprintf("unable to delete benchmark: %s", err), rw)
		return
	}

	writeJSON(http.StatusOK, map[string]string{
		"message": "emitted benchmark deletion job",
	}, rw)
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

	settings.ID = RandString(8)

	jobs, err := h.bench.GenerateCreateJobs(settings)
	if err != nil {
		h.log.Errorf("unable to create jobs: %s", err)
		writeErrorJSON(http.StatusInternalServerError, fmt.Sprintf("unable to create jobs: %s", err), rw)
		return
	}

	// Create result bucket
	bucket, err := h.nats.GetOrCreateBucket(natssvc.ResultBucketPrefix, settings.ID)
	if err != nil {
		h.log.Errorf("unable to get or create result bucket: %s", err)
		writeErrorJSON(http.StatusInternalServerError, fmt.Sprintf("unable to get or create result bucket: %s", err), rw)
		return
	}

	bucketName := fmt.Sprintf("%s-%s", natssvc.ResultBucketPrefix, settings.ID)

	h.nats.CacheBucket(bucketName, bucket)

	if err := h.nats.EmitJobs(types.CreateJob, jobs); err != nil {
		h.log.Errorf("unable to emit jobs: %s", err)
		writeErrorJSON(http.StatusInternalServerError, fmt.Sprintf("unable to emit create benchmark: %s", err), rw)
		return
	}

	if err := h.nats.SaveSettings(settings); err != nil {
		h.log.Errorf("unable to save settings: %s", err)
		writeErrorJSON(http.StatusInternalServerError, fmt.Sprintf("unable to save settings: %s", err), rw)
		return
	}

	writeJSON(http.StatusOK, map[string]string{
		"id":      settings.ID,
		"message": "benchmark created successfully",
	}, rw)
}

func validateSettings(settings *types.Settings) error {
	if settings == nil {
		return errors.New("settings cannot be nil")
	}

	if settings.Read == nil && settings.Write == nil {
		return errors.New("read or write settings must be set")
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

	if rs.NumWorkersPerStream < 1 {
		rs.NumWorkersPerStream = bench.DefaultNumWorkersPerStream
	}

	if rs.BatchSize < 1 {
		rs.BatchSize = bench.DefaultBatchSize
	}

	if rs.BatchSize > rs.NumMessagesPerStream {
		return errors.New("batch size cannot be greater than num messages per stream")
	}

	if rs.NumMessagesPerStream < 1 {
		rs.NumMessagesPerStream = bench.DefaultNumMessagesPerStream
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

	if ws.NumWorkersPerStream < 1 {
		ws.NumWorkersPerStream = bench.DefaultNumWorkersPerStream
	}

	if ws.MsgSizeBytes < 1 {
		ws.MsgSizeBytes = bench.DefaultMsgSizeBytes
	}

	if ws.NumMessagesPerStream == 0 {
		ws.NumMessagesPerStream = bench.DefaultNumMessagesPerStream
	}

	return nil
}
