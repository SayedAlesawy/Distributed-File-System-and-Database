package datanode

import (
	"time"

	"github.com/pebbe/zmq4"
)

// dataNode A struct to represent the basic structure of a Data Node
type dataNode struct {
	ip           string   //The IP of the current machine
	port         string   //The port to which the current will publish
	id           int      //A unique ID for the current machine
	trackerIP    string   //The IP of the tracker machine
	trackerPorts []string //The port of the tracker machine
}

// dtHeartbeatNode A struct to represent a data node that sends heartbeat signals
// This struct extends the dataNode struct for added functionality
type dtHeartbeatNode struct {
	publisherSocket      *zmq4.Socket  //A publisher socket to which the machine publishes
	heartbeatInterval    time.Duration //Defines the frequency at which the data node sends heartbeats
	heartbeatTrackerPort string        //The port of the heartbeat service in the tracker
	dataNode                           //Provides the basic functionality of a data node
}

// NewDataNode A constructor function for the dataNode type
func NewDataNode(_ip string, _port string, _id int, _trackerIP string, _trackerPorts []string) dataNode {
	dataNodeObj := dataNode{
		ip:           _ip,
		port:         _port,
		id:           _id,
		trackerIP:    _trackerIP,
		trackerPorts: _trackerPorts,
	}

	return dataNodeObj
}

// NewDtHeartbeatNode A constructor function for the DtHeartbeatNode type
func NewDtHeartbeatNode(_dataNodeObj dataNode, _heartbeatInterval time.Duration, _heartbeatTrackerPort string) dtHeartbeatNode {
	dtHeartbeatNodeObj := dtHeartbeatNode{
		heartbeatInterval:    _heartbeatInterval,
		dataNode:             _dataNodeObj,
		heartbeatTrackerPort: _heartbeatTrackerPort,
	}

	return dtHeartbeatNodeObj
}
