package trackernode

import (
	"time"

	"github.com/pebbe/zmq4"
)

// trackerNode A struct to represent the basic structure of a Tracker Node
type trackerNode struct {
	id           int            //ID of the tracker process
	ip           string         //The IP of the Tracker machine
	requestsPort string         //The requests port of the Tracker machine
	datanodePort string         //The datanode port on the Tracker machine
	datanodeIPs  map[int]string //Keep track of the current connected machine IPs
}

// heartbeatTrackerNode A struct to represent a Tracker Node that listens to heartbeats
//This struct extends the dataNode struct for added functionality
type heartbeatTrackerNode struct {
	subscriberSocket       *zmq4.Socket      //A susbscriber socket
	datanodeTimeStamps     map[int]time.Time //Keep track of the timestamps
	disconnectionThreshold time.Duration     //A threshold to disconnect a machine
	trackerNode
}

//NewTrackerNode A constructor function for the trackerNode type
func NewTrackerNode(_id int, _ip string, _requestsPort string, _datanodePort string) trackerNode {
	trackerNodeObj := trackerNode{
		id:           _id,
		ip:           _ip,
		requestsPort: _requestsPort,
		datanodePort: _datanodePort,
	}

	trackerNodeObj.datanodeIPs = make(map[int]string)

	return trackerNodeObj
}

// NewHeartbeatTrackerNode A constructor function for the heartbeatTrackerNode type
func NewHeartbeatTrackerNode(_trackerNodeObj trackerNode, _disconnectionThreshold time.Duration) heartbeatTrackerNode {
	heartbeatTrackerNodeObj := heartbeatTrackerNode{
		trackerNode:            _trackerNodeObj,
		disconnectionThreshold: _disconnectionThreshold,
	}

	heartbeatTrackerNodeObj.trackerNode.datanodeIPs = make(map[int]string)
	heartbeatTrackerNodeObj.datanodeTimeStamps = make(map[int]time.Time)

	return heartbeatTrackerNodeObj
}
