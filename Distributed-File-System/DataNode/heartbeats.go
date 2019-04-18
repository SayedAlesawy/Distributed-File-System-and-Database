package datanode

import (
	"fmt"
	"log"
	"time"
)

// SendHeartBeat A function to publish heartbeat signals
func (dtHeartbeatNodeObj dtHeartbeatNode) SendHeartBeat() {
	defer dtHeartbeatNodeObj.publisherSocket.Close()

	dtHeartbeatNodeObj.establishPublisherConnection()

	for range time.Tick(dtHeartbeatNodeObj.heartbeatInterval) {
		heartbeat := fmt.Sprintf("Heartbeat %d", dtHeartbeatNodeObj.dataNode.id)

		dtHeartbeatNodeObj.publisherSocket.Send(heartbeat, 0)

		log.Println("Sent", heartbeat)
	}
}
