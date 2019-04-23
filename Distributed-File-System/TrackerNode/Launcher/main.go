package main

import (
	trackernode "Distributed-Video-Processing-Cluster/Distributed-File-System/TrackerNode/Utils"
	"log"
	"os/exec"
	"sync"
	"time"
)

// Master Tracker data
const masterTrackerIP string = "127.0.0.1"
const ipListenerPort string = "9000"
const masterTrackerID int = 0

func launchTrackers() {
	log.Println(trackernode.LogSignL, "Launching Tracker Processes")

	sideTrackerDNIDs := []string{"1", "2"}
	sideTrackerDNPorts := []string{"9001", "9002"}
	sideTrackerReqPorts := []string{"8001", "8002"}
	path := "../TRLauncher/main.go"

	for i := 0; i < 2; i++ {
		params := masterTrackerIP + " " + sideTrackerDNIDs[i] + " " + sideTrackerDNPorts[i] + " " + sideTrackerReqPorts[i]
		command := "go run " + path + " " + params

		cmd := exec.Command("gnome-terminal", "--title=Tracker"+sideTrackerDNIDs[i], "-e", command)
		err := cmd.Start()

		if err != nil {
			log.Println(trackernode.LogSignL, "Error starting Tracker Process #", sideTrackerDNIDs[i])
			return
		}

		log.Println(trackernode.LogSignL, "Launched Tracker Process#", sideTrackerDNIDs[i])
	}

	log.Println(trackernode.LogSignL, "is all set!")
}

func main() {
	disconnectionThreshold := time.Duration(2000000001)

	trackerNodeLauncherObj := trackernode.NewTrackerNodeLauncher(masterTrackerID, masterTrackerIP, disconnectionThreshold, ipListenerPort)

	log.Println(trackernode.LogSignL, "Successfully launched")

	launchTrackers()

	var portsMutex sync.Mutex
	var timeStampsMutex sync.Mutex

	go trackerNodeLauncherObj.ReceiveHandshake(&portsMutex, &timeStampsMutex)

	go trackerNodeLauncherObj.UpdateDataNodeAliveStatus(&portsMutex, &timeStampsMutex)

	trackerNodeLauncherObj.ListenToHeartbeats(&portsMutex, &timeStampsMutex)
}
