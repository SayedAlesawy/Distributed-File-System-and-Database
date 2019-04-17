package main

import (
	trackernode "Distributed-Video-Processing-Cluster/Distributed-File-System/TrackerNode"
	"log"
)

func main() {
	ip := "127.0.0.1"
	port := "9092"

	trackerNodeObj := trackernode.NewTrackerNode(ip, port)

	trackerHeartbeatNodeObj := trackernode.NewHeartbeatTrackerNode(trackerNodeObj)

	log.Println("[Heartbeat Tracker Node]", "Successfully launched")

	//trackerHeartbeatNodeObj.RecieveIP()

	trackerHeartbeatNodeObj.ListenToHeartbeats()
}
