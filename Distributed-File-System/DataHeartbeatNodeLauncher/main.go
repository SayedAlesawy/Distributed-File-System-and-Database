package main

import (
	datanode "Distributed-Video-Processing-Cluster/Distributed-File-System/DataNode"
	"fmt"
	"log"
	"time"
)

func main() {
	//Tracker data
	trackerIP := "127.0.0.1"
	trackerPort := "9092"

	ip := "127.0.0.1"
	port := ""
	id := 1
	heartbeatInterval := time.Second

	log.Print("Port = ")
	fmt.Scanf("%s", &port)

	log.Print("ID = ")
	fmt.Scanf("%d", &id)

	dataNodeObj := datanode.NewDataNode(ip, port, id, trackerIP, trackerPort)

	dtHeartbeatNodeObj := datanode.NewDtHeartbeatNode(dataNodeObj, heartbeatInterval)

	log.Println("[Heartbeat Data Node #]", id, "Successfully launched")

	dtHeartbeatNodeObj.SendIP()

	dtHeartbeatNodeObj.SendHeartBeat()
}
