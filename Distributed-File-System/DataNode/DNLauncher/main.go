package main

import (
	datanode "Distributed-Video-Processing-Cluster/Distributed-File-System/DataNode/Utils"
	"fmt"
	"log"
	"os"
	"strconv"
)

var trackerIP = ""
var trackerDNPorts = []string{"", ""}

func main() {
	//Receive command line params
	args := os.Args
	fmt.Println(args[1:])

	//Tracker Data
	trackerIP = args[1]
	trackerDNPorts[0] = args[2]
	trackerDNPorts[1] = args[3]

	//Data Node Data
	ip := args[4]
	id, _ := strconv.Atoi(args[5])
	port := args[6]

	dataNodeObj := datanode.NewDataNode(id, ip, port, trackerIP, trackerDNPorts)

	log.Println(datanode.LogSignDN, "#", id, "Successfully launched")

	dataNodeObj.DoWork()
}
