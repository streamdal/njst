package natssvc

import (
	"github.com/nats-io/nats.go"
)

func (n *NATSService) createHandler(msg *nats.Msg) {
	n.log.Debugf("createHandler: (nodeID: %s) received message on subject %s", n.params.NodeID, msg.Subject)
}

func (n *NATSService) deleteHandler(msg *nats.Msg) {
	n.log.Debugf("deleteHandler: (nodeID: %s) received message on subject %s", n.params.NodeID, msg.Subject)
}

func (n *NATSService) statusHandler(msg *nats.Msg) {
	n.log.Debugf("statusHandler: (nodeID: %s) received message on subject %s", n.params.NodeID, msg.Subject)
}
