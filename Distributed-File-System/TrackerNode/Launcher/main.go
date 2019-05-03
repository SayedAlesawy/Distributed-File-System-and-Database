package main

import (
	trackernode "Distributed-Video-Processing-Cluster/Distributed-File-System/TrackerNode/Utils"
	constants "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Constants"
	dbwrapper "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Database"

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

	db := dbwrapper.ConnectDB()
	defer db.Close()

	dbwrapper.CleanUP(db, trackernode.SQLDropDataNodesTable+trackernode.SQLDropMetaFileTable)
	dbwrapper.Migrate(db, trackernode.SQLCreateDataNodesTable+trackernode.SQLCreateMetaFile)

	var portsMutex sync.Mutex
	var ipMutex sync.Mutex
	var timeStampsMutex sync.Mutex
	var dbMutex sync.Mutex

	trackerNodeLauncherObj := trackernode.NewTrackerNodeLauncher(masterTrackerID, masterTrackerIP, disconnectionThreshold,
		ipListenerPort, db, &timeStampsMutex, &ipMutex, &portsMutex, &dbMutex)

	log.Println(trackernode.LogSignL, "Successfully launched")

	launchTrackers()

	go trackerNodeLauncherObj.ReceiveHandshake()

	go trackerNodeLauncherObj.UpdateDataNodeAliveStatus()

	trackerNodeLauncherObj.ListenToHeartbeats()
}
