package trackernode

import (
	"time"

	"github.com/pebbe/zmq4"
)

// LogSignL Used for logging Launcher messages
const LogSignL string = "[Tracker Launcher]"

// LogSignTR Used for logging Tracker messages
const LogSignTR string = "[Tracker]"

// trackerNode A struct to represent the basic structure of a Tracker Node
type trackerNode struct {
	id           int    //ID of the tracker process
	ip           string //The IP of the Tracker machine
	requestsPort string //The requests port of the Tracker machine
	datanodePort string //The datanode port on the Tracker machine
}

// heartbeatTrackerNode A struct to represent a Tracker Node that listens to heartbeats
//This struct extends the dataNode struct for added functionality
type trackerNodeLauncher struct {
	id                     int               //Unique ID
	ip                     string            //Tracker IP
	trackerIPsPort         string            //A port on which the tracker listens for incoming IPs
	subscriberSocket       *zmq4.Socket      //A susbscriber socket
	disconnectionThreshold time.Duration     //A threshold to disconnect a machine
	datanodeTimeStamps     map[int]time.Time //Keep track of the timestamps
	datanodeBasePorts      map[int]string    //Keep track of the datanode base ports
}

//NewTrackerNode A constructor function for the trackerNode type
func NewTrackerNode(_id int, _ip string, _requestsPort string, _datanodePort string) trackerNode {
	trackerNodeObj := trackerNode{
		id:           _id,
		ip:           _ip,
		requestsPort: _requestsPort,
		datanodePort: _datanodePort,
	}

	return trackerNodeObj
}

// NewTrackerNodeLauncher A constructor function for the trackerNodeLauncher type
func NewTrackerNodeLauncher(_id int, _ip string, _disconnectionThreshold time.Duration, _trackerIPsPort string) trackerNodeLauncher {
	trackerNodeLauncherObj := trackerNodeLauncher{
		id:                     _id,
		ip:                     _ip,
		trackerIPsPort:         _trackerIPsPort,
		disconnectionThreshold: _disconnectionThreshold,
	}

	trackerNodeLauncherObj.datanodeTimeStamps = make(map[int]time.Time)
	trackerNodeLauncherObj.datanodeBasePorts = make(map[int]string)

	return trackerNodeLauncherObj
}
