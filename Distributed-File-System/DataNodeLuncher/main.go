package main

import (
	"Distributed-Video-Processing-Cluster/Distributed-File-System/datanode"
	"time"
)

func main() {
	ip := "127.0.0.1"
	port := "9092"
	id := 1
	heartbeatInterval := time.Second

	dataNodeObj := datanode.New(ip, port, id, heartbeatInterval)

	dataNodeObj.SendHeartBeat()
}
