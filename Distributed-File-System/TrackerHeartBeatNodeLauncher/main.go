package main

import (
	trackernode "Distributed-Video-Processing-Cluster/Distributed-File-System/TrackerNode"
	"log"
	"sync"
	"time"
)

func main() {
	ip := "127.0.0.1"
	id := 0
	datanodePort := "9092"

	disconnectionThreshold := time.Duration(2000000001)
	trackerNodeObj := trackernode.NewTrackerNode(id, ip, "", datanodePort)

	trackerHeartbeatNodeObj := trackernode.NewHeartbeatTrackerNode(trackerNodeObj, disconnectionThreshold)

	log.Println("[Heartbeat Tracker Node]", "Successfully launched")

	var IPsMutex sync.Mutex
	var timeStampsMutex sync.Mutex

	go trackerHeartbeatNodeObj.RecieveHeartbeatNodeIPs(&IPsMutex, &timeStampsMutex)

	go trackerHeartbeatNodeObj.UpdateDataNodeAliveStatus(&IPsMutex, &timeStampsMutex)

	trackerHeartbeatNodeObj.ListenToHeartbeats(&IPsMutex, &timeStampsMutex)
}
