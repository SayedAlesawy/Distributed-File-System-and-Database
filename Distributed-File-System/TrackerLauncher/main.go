package main

import (
	trackernode "Distributed-Video-Processing-Cluster/Distributed-File-System/TrackerNode"
	"fmt"
	"log"
)

func main() {
	ip := "127.0.0.1"
	port := ""
	id := 1

	log.Println("[Tracker]", "Port = ")
	fmt.Scanf("%s", &port)

	log.Println("[Tracker]", "ID = ")
	fmt.Scanf("%d", &id)

	trackerNodeObj := trackernode.NewTrackerNode(id, ip, port)

	log.Println("[Tracker]", "Successfully launched")

	trackerNodeObj.ListenToClientRequests()
}
