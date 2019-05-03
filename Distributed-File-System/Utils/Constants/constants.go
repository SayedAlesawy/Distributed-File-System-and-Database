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
var TrackerResponse = DataNodeLauncherIP + " " + "7011"

// ReplicationRoutineFrequency The time after which the replication routine runs
var ReplicationRoutineFrequency = time.Duration(1 * time.Minute)

var DownloadIP1 = DataNodeLauncherIP
var DownloadPort1 = "701"
var DownloadIP2 = DataNodeLauncherIP
var DownloadPort2 = "702"
var DownloadIP3 = DataNodeLauncherIP
var DownloadPort3 = "601"
var DownloadIP4 = DataNodeLauncherIP
var DownloadPort4 = "602"
var DownloadIP5 = DataNodeLauncherIP
var DownloadPort5 = "501"
var DownloadIP6 = DataNodeLauncherIP
var DownloadPort6 = "502"

//----------------------------------------------------------------------

// DataNodeLauncherIP The IP of a single Data Node
var DataNodeLauncherIP = "127.0.0.1"

//----------------------------------------------------------------------

// ClientIP Client IP
var ClientIP = "127.0.0.1"
