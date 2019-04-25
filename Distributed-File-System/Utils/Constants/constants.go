package constants

import "time"

// TrackerIP Tracker machine IP
var TrackerIP = "127.0.0.1"

// TrackerReqPorts Tracker requests ports [used by clients]
var TrackerReqPorts = []string{"8001", "8002"}

// TrackerDNPorts Tracker data node ports
var TrackerDNPorts = []string{"9001", "9002"}

// TrackerIPsPort A port on which the tracker receives IP handshakes
var TrackerIPsPort = "9000"

// MasterTrackerID The process ID of the master tracker
var MasterTrackerID = 0

// DisconnectionThreshold The time after which we consider a data node offline
var DisconnectionThreshold = time.Duration(2*time.Second + 1)

// TrackerResponse A temporary tracker response
var TrackerResponse = DataNodeLauncherIP + " " + "7012" + " " + "7011"

// ReplicationRoutineFrequency The time after which the replication routine runs
var ReplicationRoutineFrequency = time.Duration(time.Minute)

//----------------------------------------------------------------------

// DataNodeLauncherIP The IP of a single Data Node
var DataNodeLauncherIP = "127.0.0.1"

//----------------------------------------------------------------------

// ClientIP Client IP
var ClientIP = "127.0.0.1"
