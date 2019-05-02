package main

import (
	trackernode "Distributed-Video-Processing-Cluster/Distributed-File-System/TrackerNode/Utils"
	dbwrapper "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Database"
	"fmt"
	"log"
	"os"
	"strconv"
)

func main() {
	args := os.Args
	fmt.Println(args[1:])

	//Tracker parameters
	ip := args[1]
	id, _ := strconv.Atoi(args[2])
	dnPort := args[3]
	reqPort := args[4]

	db := dbwrapper.ConnectDB()
	defer db.Close()

	trackerNodeObj := trackernode.NewTrackerNode(id, ip, reqPort, dnPort, db)

	log.Println(trackernode.LogSignTR, args[2], "Successfully launched")

	if id == 1 {
		go trackerNodeObj.Replicate()
	}

	trackerNodeObj.ListenToClientRequests()
}
