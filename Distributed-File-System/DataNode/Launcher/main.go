package main

import (
	datanode "Distributed-Video-Processing-Cluster/Distributed-File-System/DataNode/Utils"
	"log"
	"os"
	"os/exec"
	"strconv"
	"time"
)

//DataNode Launcher Data
const dataNodeLauncherIP string = "127.0.0.1"
const heartbeatInterval time.Duration = time.Second

//Tracker Data
const trackerIP string = "127.0.0.1"
const trackerIPsPort string = "9000"

var trackerDNPorts = []string{"9001", "9002"}

func getTrackerParams() string {
	trackerParams := trackerIP + " " + trackerDNPorts[0] + " " + trackerDNPorts[1]

	return trackerParams
}

func launchDataNodes(launcherID string, launcherPort string) {
	log.Println(datanode.LogSignL, "Launching Data Nodes Processes")

	dataNodePorts := []string{launcherPort + "1", launcherPort + "2"}
	dataNodeReqPorts := []string{launcherPort + "3", launcherPort + "4"}

	path := "../DNLauncher/main.go"

	for i := 0; i < 2; i++ {
		params := getTrackerParams() + " " + dataNodeLauncherIP + " " + launcherID + " " +
			dataNodePorts[i] + " " + dataNodeReqPorts[i]
		command := "go run " + path + " " + params

		cmd := exec.Command("gnome-terminal", "--title=DataNode"+launcherID, "-e", command)
		err := cmd.Start()

		if err != nil {
			log.Println(datanode.LogSignL, "Error starting Data Node Process#", i+1)
			return
		}

		log.Println(datanode.LogSignL, "Launched Data Node Process#", i+1)
	}

	log.Println(datanode.LogSignL, "is all set!")
}

func getHandshake(launcherID string, launcherPort string) string {
	hbIP := dataNodeLauncherIP + ":" + launcherPort + "0"
	dn1IP := dataNodeLauncherIP + ":" + launcherPort + "1"
	dn2IP := dataNodeLauncherIP + ":" + launcherPort + "2"

	handshake := hbIP + " " + dn1IP + " " + dn2IP + " " + launcherID

	return handshake
}

func main() {
	//Receive command line params
	args := os.Args

	//DataNode Launcher Params
	dataNodeLauncherID, _ := strconv.Atoi(args[1])
	dataNodeLauncherPort := args[2] //Sent to the tracker as handshake

	dataNodeObj := datanode.NewDataNode(dataNodeLauncherID, dataNodeLauncherIP, dataNodeLauncherPort+"0", "",
		trackerIP, trackerDNPorts)

	dataNodeLauncherObj := datanode.NewDataNodeLauncher(dataNodeObj, heartbeatInterval, dataNodeLauncherPort+"0",
		trackerIPsPort)

	log.Println(datanode.LogSignL, "#", dataNodeLauncherID, "Successfully launched")

	launchDataNodes(args[1], dataNodeLauncherPort)

	dataNodeLauncherObj.SendHandshake(getHandshake(args[1], dataNodeLauncherPort))

	dataNodeLauncherObj.SendHeartBeat()
}
