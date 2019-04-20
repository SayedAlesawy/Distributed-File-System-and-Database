package datanode

import (
	"fmt"
	"log"
	"time"
)

// SendHeartBeat A function to publish heartbeat signals
func (dataNodeLauncherObj *dataNodeLauncher) SendHeartBeat() {
	defer dataNodeLauncherObj.publisherSocket.Close()

	dataNodeLauncherObj.establishPublisherConnection()

	for range time.Tick(dataNodeLauncherObj.heartbeatInterval) {
		heartbeat := fmt.Sprintf("Heartbeat %d", dataNodeLauncherObj.dataNode.id)

		dataNodeLauncherObj.publisherSocket.Send(heartbeat, 0)

		log.Println(LogSignL, dataNodeLauncherObj.dataNode.id, "Sent", "Heartbeat")
	}
}
