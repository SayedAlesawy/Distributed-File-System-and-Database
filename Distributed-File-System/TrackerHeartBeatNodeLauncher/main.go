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
	heartbeatPort := "9092"

	disconnectionThreshold := time.Duration(2000000001)
	trackerNodeObj := trackernode.NewTrackerNode(id, ip, heartbeatPort)

	trackerHeartbeatNodeObj := trackernode.NewHeartbeatTrackerNode(trackerNodeObj, disconnectionThreshold)

	log.Println("[Heartbeat Tracker Node]", "Successfully launched")

	var IPsMutex sync.Mutex
	var timeStampsMutex sync.Mutex

	go trackerHeartbeatNodeObj.RecieveIP(&IPsMutex, &timeStampsMutex)

	go trackerHeartbeatNodeObj.UpdateDataNodeAliveStatus(&IPsMutex, &timeStampsMutex)

	trackerHeartbeatNodeObj.ListenToHeartbeats(&IPsMutex, &timeStampsMutex)
}
