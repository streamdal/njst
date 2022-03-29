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
	id := ps.ByName("id")

	if id == "" {
		writeErrorJSON(http.StatusBadRequest, "id is required", rw)
		return
	}

	status, err := h.bench.Status(id)
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
	id := ps.ByName("id")

	if id == "" {
		writeErrorJSON(http.StatusBadRequest, "id is required", rw)
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

	settings.BenchID = natssvc.RandID(8)

	bench.SetDefaultSettings(settings)

	// Only a single node should ever attempt to create all of the streams
	natsSettings, err := h.nats.SetupBench(settings)
	if err != nil {
		h.log.Errorf("unable to create streams: %s", err)
		writeErrorJSON(http.StatusInternalServerError, fmt.Sprintf("unable to create streams: %s", err), rw)
		return
	}

	if err := h.nats.EmitCreateBenchmark(natsSettings); err != nil {
		h.log.Errorf("unable to emit create benchmark: %s", err)
		writeErrorJSON(http.StatusInternalServerError, fmt.Sprintf("unable to emit create benchmark: %s", err), rw)
		return
	}

	writeJSON(http.StatusOK, map[string]string{"id": id}, rw)
}

func validateSettings(settings *types.Settings) error {
	if settings == nil {
		return errors.New("settings cannot be nil")
	}

	if settings.Consumer == nil && settings.Producer == nil {
		return errors.New("consumer, producer or both settings must be set")
	}

	return nil
}
