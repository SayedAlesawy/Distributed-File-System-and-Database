package datanode

import (
	"time"

	"github.com/pebbe/zmq4"
)

// LogSignL Used for logging Launcher messages
const LogSignL string = "[Data Node Launcher]"

// LogSignDN Used for logging Data Node messages
const LogSignDN string = "[Data Node]"

// dataNode A struct to represent the basic structure of a Data Node
type dataNode struct {
	id           int      //A unique ID for the current machine
	ip           string   //The IP of the current machine
	reqPort      string   //Request port
	upPort       string   //Port that handles upload requests
	downPort     string   //Port that handles download requests
	repUpPort    string   //Port that handles outgoing replication requests
	repDownPort  string   //Port that handles incoming replication requests
	trackerIP    string   //The IP of the tracker machine
	trackerPorts []string //The ports of the tracker machine (processes)
}

// dtHeartbeatNode A struct to represent a data node that sends heartbeat signals
// This struct extends the dataNode struct for added functionality
type dataNodeLauncher struct {
	id                int           //Unique ID
	ip                string        //Launcher IP
	trackerIP         string        //Tracker IP
	publisherSocket   *zmq4.Socket  //A publisher socket to which the machine publishes heartbeats
	heartbeatInterval time.Duration //Defines the frequency at which the launcer publishes heartbeats
	trackerIPsPort    string        //The port on which the tracker machine receives IPs
	heartbeatPort     string        //The port on which the launcher publishes heartbeats
}

// NewDataNode A constructor function for the dataNode type
func NewDataNode(_id int, _ip string, _reqPort string, _upPort string, _downPort string, _repUpPort string,
	_repDownPort string, _trackerIP string, _trackerPorts []string) dataNode {
	dataNodeObj := dataNode{
		id:           _id,
		ip:           _ip,
		reqPort:      _reqPort,
		upPort:       _upPort,
		downPort:     _downPort,
		repUpPort:    _repUpPort,
		repDownPort:  _repDownPort,
		trackerIP:    _trackerIP,
		trackerPorts: _trackerPorts,
	}

	return dataNodeObj
}

// NewDataNodeLauncher A constructor function for the DataNode Launcher type
func NewDataNodeLauncher(_id int, _ip string, _trackerIP string, _heartbeatInterval time.Duration,
	_heartbeatPort string, _trackerIPsPort string) dataNodeLauncher {
	dataNodeLauncher := dataNodeLauncher{
		id:                _id,
		ip:                _ip,
		trackerIP:         _trackerIP,
		heartbeatInterval: _heartbeatInterval,
		heartbeatPort:     _heartbeatPort,
		trackerIPsPort:    _trackerIPsPort,
	}

	return dataNodeLauncher
}
