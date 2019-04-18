package trackernode

import (
	"time"

	"github.com/pebbe/zmq4"
)

// trackerNode A struct to represent the basic structure of a Tracker Node
type trackerNode struct {
	ip   string //The IP of the Tracker machine
	port string //The port of the Tracker machine
}

// heartbeatTrackerNode A struct to represent a Tracker Node that listens to heartbeats
//This struct extends the dataNode struct for added functionality
type heartbeatTrackerNode struct {
	subscriberSocket       *zmq4.Socket      //A susbscriber socket
	datanodeIPs            map[int]string    //Keep track of the current connected machine IPs
	datanodeTimeStamps     map[int]time.Time //Keep track of the timestamps
	disconnectionThreshold time.Duration     //A threshold to disconnect a machine
	trackerNode
}

//NewTrackerNode A constructor function for the trackerNode type
func NewTrackerNode(_ip string, _port string) trackerNode {
	trackerNodeObj := trackerNode{
		ip:   _ip,
		port: _port,
	}

	return trackerNodeObj
}

// NewHeartbeatTrackerNode A constructor function for the heartbeatTrackerNode type
func NewHeartbeatTrackerNode(_trackerNodeObj trackerNode, _disconnectionThreshold time.Duration) heartbeatTrackerNode {
	heartbeatTrackerNodeObj := heartbeatTrackerNode{
		trackerNode:            _trackerNodeObj,
		disconnectionThreshold: _disconnectionThreshold,
	}

	heartbeatTrackerNodeObj.datanodeIPs = make(map[int]string)
	heartbeatTrackerNodeObj.datanodeTimeStamps = make(map[int]time.Time)

	return heartbeatTrackerNodeObj
}
