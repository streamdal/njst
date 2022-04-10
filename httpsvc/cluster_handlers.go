package httpsvc

import (
	"fmt"
	"net/http"
)

type GetClusterResponse struct {
	Nodes []string `json:"nodes"`
	Count int      `json:"count"`
}

func (h *HTTPService) getClusterHandler(rw http.ResponseWriter, r *http.Request) {
	nodes, err := h.nats.GetNodeList()
	if err != nil {
		writeErrorJSON(http.StatusInternalServerError, fmt.Sprintf("unable to get cluster nodes: %v", err), rw)
		return
	}

	resp := &GetClusterResponse{
		Nodes: nodes,
		Count: len(nodes),
	}

	writeJSON(http.StatusOK, resp, rw)
}
