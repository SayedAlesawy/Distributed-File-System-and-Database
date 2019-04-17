package main

import (
	datanode "Distributed-Video-Processing-Cluster/Distributed-File-System/DataNode"
	"log"
	"time"
)

func main() {
	ip := "127.0.0.1"
	port := "9091"
	trackerIP := "127.0.0.1"
	trackerPort := "9092"
	id := 1
	heartbeatInterval := time.Second

	dataNodeObj := datanode.NewDataNode(ip, port, id, trackerIP, trackerPort)

	dtHeartbeatNodeObj := datanode.NewDtHeartbeatNode(dataNodeObj, heartbeatInterval)

	log.Println("[Heartbeat Data Node]", "Successfully launched")

	dtHeartbeatNodeObj.SendIP()

	dtHeartbeatNodeObj.SendHeartBeat()
}
