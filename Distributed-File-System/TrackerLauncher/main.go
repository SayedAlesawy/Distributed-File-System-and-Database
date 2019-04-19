package main

import (
	trackernode "Distributed-Video-Processing-Cluster/Distributed-File-System/TrackerNode"
	"fmt"
	"log"
	"sync"
)

func main() {
	ip := "127.0.0.1"
	requestsPort := ""
	datanodePort := ""
	id := 1

	log.Println("[Tracker]", "ID = ")
	fmt.Scanf("%d", &id)

	log.Println("[Tracker]", "Requests Port = ")
	fmt.Scanf("%s", &requestsPort)

	log.Println("[Tracker]", "Datanodes Port = ")
	fmt.Scanf("%s", &datanodePort)

	trackerNodeObj := trackernode.NewTrackerNode(id, ip, requestsPort, datanodePort)

	log.Println("[Tracker]", "Successfully launched")

	var IPsMutex sync.Mutex

	go trackerNodeObj.RecieveDataNodeIPs(&IPsMutex)

	trackerNodeObj.ListenToClientRequests()
}
