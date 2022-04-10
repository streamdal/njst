package httpsvc

import (
	"net/http"
)

func (h *HTTPService) healthCheckHandler(rw http.ResponseWriter, r *http.Request) {
	status := http.StatusOK
	body := "ok"

	rw.WriteHeader(status)

	if _, err := rw.Write([]byte(body)); err != nil {
		h.log.Errorf("unable to write health output: %s", err)
	}
}

func (h *HTTPService) versionHandler(rw http.ResponseWriter, r *http.Request) {
	rw.Header().Set("Content-Type", "application/json; charset=UTF-8")
	rw.WriteHeader(http.StatusOK)

	writeJSON(http.StatusOK, map[string]string{"version": h.version}, rw)
}
