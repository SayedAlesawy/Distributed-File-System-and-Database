package main

import (
	trackernode "Distributed-Video-Processing-Cluster/Distributed-File-System/TrackerNode/Utils"
	"fmt"
	"log"
	"os"
	"strconv"
)

const logSign string = "Tracker"

func main() {
	args := os.Args
	fmt.Println(args[1:])

	//Tracker parameters
	ip := args[1]
	id, _ := strconv.Atoi(args[2])
	dnPort := args[3]
	reqPort := args[4]

	trackerNodeObj := trackernode.NewTrackerNode(id, ip, reqPort, dnPort)

	log.Println(logSign+"#"+args[2], "Successfully launched")

	//var IPsMutex sync.Mutex

	//go trackerNodeObj.RecieveDataNodeIPs(&IPsMutex)

	trackerNodeObj.ListenToClientRequests()
}
