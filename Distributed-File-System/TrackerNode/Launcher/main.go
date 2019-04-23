package main

import (
	trackernode "Distributed-Video-Processing-Cluster/Distributed-File-System/TrackerNode/Utils"
	constants "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Constants"
	"log"
	"os/exec"
	"sync"
)

// Master Tracker data
var masterTrackerIP = constants.TrackerIP
var ipListenerPort = constants.TrackerIPsPort
var masterTrackerID = constants.MasterTrackerID

func launchTrackers() {
	log.Println(trackernode.LogSignL, "Launching Tracker Processes")

	sideTrackerDNIDs := []string{"1", "2"}
	sideTrackerDNPorts := constants.TrackerDNPorts
	sideTrackerReqPorts := constants.TrackerReqPorts
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
	disconnectionThreshold := constants.DisconnectionThreshold

	trackerNodeLauncherObj := trackernode.NewTrackerNodeLauncher(masterTrackerID, masterTrackerIP, disconnectionThreshold, ipListenerPort)

	log.Println(trackernode.LogSignL, "Successfully launched")

	launchTrackers()

	var portsMutex sync.Mutex
	var timeStampsMutex sync.Mutex

	go trackerNodeLauncherObj.ReceiveHandshake(&portsMutex, &timeStampsMutex)

	go trackerNodeLauncherObj.UpdateDataNodeAliveStatus(&portsMutex, &timeStampsMutex)

	trackerNodeLauncherObj.ListenToHeartbeats(&portsMutex, &timeStampsMutex)
}
