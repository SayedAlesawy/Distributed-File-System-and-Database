package datanode

import (
	logger "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Log"
	"fmt"
	"time"
)

// SendHeartBeat A function to publish heartbeat signals
func (dataNodeLauncherObj *dataNodeLauncher) SendHeartBeat() {
	defer dataNodeLauncherObj.publisherSocket.Close()

	dataNodeLauncherObj.establishPublisherConnection()

	for range time.Tick(dataNodeLauncherObj.heartbeatInterval) {
		heartbeat := fmt.Sprintf("Heartbeat %d", dataNodeLauncherObj.id)

		dataNodeLauncherObj.publisherSocket.Send(heartbeat, 0)

		logger.LogMsg(LogSignL, dataNodeLauncherObj.id, "Sent Heartbeat")
	}
}
