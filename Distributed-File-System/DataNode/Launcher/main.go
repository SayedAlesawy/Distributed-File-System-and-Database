package main

import (
	datanode "Distributed-Video-Processing-Cluster/Distributed-File-System/DataNode/Utils"
	"fmt"
	"log"
	"time"
)

func main() {
	//Tracker data
	trackerIP := "127.0.0.1"
	trackerPorts := []string{"8001", "8002"}
	heartbeatTrackerPort := "9000"

	ip := "127.0.0.1"
	port := ""
	id := 1
	heartbeatInterval := time.Second

	log.Print("Port = ")
	fmt.Scanf("%s", &port)

	log.Print("ID = ")
	fmt.Scanf("%d", &id)

	dataNodeObj := datanode.NewDataNode(ip, port, id, trackerIP, trackerPorts)

	dtHeartbeatNodeObj := datanode.NewDtHeartbeatNode(dataNodeObj, heartbeatInterval, heartbeatTrackerPort)

	log.Println("[Heartbeat Data Node #]", id, "Successfully launched")

	dtHeartbeatNodeObj.SendHeartbeatIP()

	dtHeartbeatNodeObj.SendHeartBeat()
}
