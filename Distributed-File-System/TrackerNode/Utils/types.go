package trackernode

import (
	"strconv"
	"strings"
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

// ReplicationRequest A struct to represent a replication request issued from Tracker and handled from Data Node
type ReplicationRequest struct {
	ID              int    //The ID of the replication request
	ClientID        int    //The client ID associated with the replicated file
	FileName        string //The file name to be replicated
	SourceID        int    //The ID of the source Data Node
	TargetNodeID    int    //The ID of the target machine
	TargetNodeIP    string //The IP of the target machine (connect there)
	TargetNodeRPort string //The replication port of the target machine (connect there)
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

// SerializeRequest A function to serialize a request structure
func SerializeRequest(request ReplicationRequest) string {
	serializedRequest := strconv.Itoa(request.ID) + " " + strconv.Itoa(request.ClientID) + " " +
		request.FileName + " " + strconv.Itoa(request.SourceID) + " " + strconv.Itoa(request.TargetNodeID) + " " +
		request.TargetNodeIP + " " + request.TargetNodeRPort

	return serializedRequest
}

// DeserializeRequest A function to deserialize a request structure
func DeserializeRequest(serializedRequest string) ReplicationRequest {
	fields := strings.Fields(serializedRequest)
	requestID, _ := strconv.Atoi(fields[0])
	clientID, _ := strconv.Atoi(fields[1])
	sourceID, _ := strconv.Atoi(fields[3])
	targetNodeID, _ := strconv.Atoi(fields[4])

	requestObj := ReplicationRequest{
		ID:              requestID,
		ClientID:        clientID,
		FileName:        fields[2],
		SourceID:        sourceID,
		TargetNodeID:    targetNodeID,
		TargetNodeIP:    fields[5],
		TargetNodeRPort: fields[6],
	}

	return requestObj
}
