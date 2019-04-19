package main

import (
	datanode "Distributed-Video-Processing-Cluster/Distributed-File-System/DataNode/Utils"
	"fmt"
	"log"
)

func main() {
	//Tracker data
	trackerIP := "127.0.0.1"
	trackerPorts := []string{"8001", "8002"}

	ip := "127.0.0.1"
	port := ""
	id := 1

	log.Print("Port = ")
	fmt.Scanf("%s", &port)

	log.Print("ID = ")
	fmt.Scanf("%d", &id)

	dataNodeObj := datanode.NewDataNode(ip, port, id, trackerIP, trackerPorts)

	log.Println("[Data Node #]", id, "Successfully launched")

	dataNodeObj.SendDataNodeIPs()

	for {
		//Do Data Node work [Listen to master tracker, and listen to clients]
	}
}
