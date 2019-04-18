package main

import (
	trackernode "Distributed-Video-Processing-Cluster/Distributed-File-System/TrackerNode"
	"log"
	"sync"
)

func main() {
	ip := "127.0.0.1"
	port := "9092"

	trackerNodeObj := trackernode.NewTrackerNode(ip, port)

	trackerHeartbeatNodeObj := trackernode.NewHeartbeatTrackerNode(trackerNodeObj)

	log.Println("[Heartbeat Tracker Node]", "Successfully launched")

	var IPsMutex sync.Mutex
	var timeStampsMutex sync.Mutex

	go trackerHeartbeatNodeObj.RecieveIP(&IPsMutex, &timeStampsMutex)

	go trackerHeartbeatNodeObj.UpdateDataNodeAliveStatus(&IPsMutex, &timeStampsMutex)

	trackerHeartbeatNodeObj.ListenToHeartbeats(&IPsMutex, &timeStampsMutex)
}
