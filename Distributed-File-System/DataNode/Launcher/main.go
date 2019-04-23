package main

import (
	datanode "Distributed-Video-Processing-Cluster/Distributed-File-System/DataNode/Utils"
	constants "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Constants"
	"log"
	"os"
	"os/exec"
	"strconv"
	"time"
)

//DataNode Launcher Data
var dataNodeLauncherIP = constants.DataNodeLauncherIP
var heartbeatInterval = time.Second

//Tracker Data
var trackerIP = constants.TrackerIP

var trackerIPsPort = constants.TrackerIPsPort

var trackerDNPorts = constants.TrackerDNPorts

func getTrackerParams() string {
	trackerParams := trackerIP + " " + trackerDNPorts[0] + " " + trackerDNPorts[1]

	return trackerParams
}

func launchDataNodes(launcherID string, launcherPort string) {
	log.Println(datanode.LogSignL, "Launching Data Nodes Processes")

	reqPorts := []string{launcherPort + "11", launcherPort + "21"}
	upPorts := []string{launcherPort + "12", launcherPort + "22"}
	downPorts := []string{launcherPort + "13", launcherPort + "23"}
	repUpPorts := []string{launcherPort + "14", launcherPort + "24"}
	repDownPorts := []string{launcherPort + "15", launcherPort + "25"}

	path := "../DNLauncher/main.go"

	for i := 0; i < 2; i++ {
		params := getTrackerParams() + " " + dataNodeLauncherIP + " " + launcherID + " " +
			reqPorts[i] + " " + upPorts[i] + " " + downPorts[i] + " " + repUpPorts[i] + " " + repDownPorts[i]
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
	handshake := dataNodeLauncherIP + " " + launcherID + " " + launcherPort

	return handshake
}

func main() {
	//Receive command line params
	args := os.Args

	//DataNode Launcher Params
	dataNodeLauncherID, _ := strconv.Atoi(args[1])
	dataNodeLauncherPort := args[2] //Sent to the tracker as handshake

	dataNodeLauncherObj := datanode.NewDataNodeLauncher(dataNodeLauncherID, dataNodeLauncherIP, trackerIP,
		heartbeatInterval, dataNodeLauncherPort+"00", trackerIPsPort)

	log.Println(datanode.LogSignL, "#", dataNodeLauncherID, "Successfully launched")

	launchDataNodes(args[1], dataNodeLauncherPort)

	dataNodeLauncherObj.SendHandshake(getHandshake(args[1], dataNodeLauncherPort))

	dataNodeLauncherObj.SendHeartBeat()
}
