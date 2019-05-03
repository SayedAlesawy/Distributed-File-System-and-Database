package request

import (
	logger "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Log"
	"fmt"
	"strconv"
	"strings"
)

// SerializeReplication A function to serialize replication requests
func SerializeReplication(request ReplicationRequest) string {
	serializedRequest := string(request.Type) + " " +
		strconv.Itoa(request.ID) + " " +
		strconv.Itoa(request.ClientID) + " " +
		request.FileName + " " +
		strconv.Itoa(request.SourceID) + " " +
		strconv.Itoa(request.TargetNodeID) + " " +
		request.TargetNodeIP + " " +
		request.TargetNodeBasePort + " " +
		request.TrackerPort

	return serializedRequest
}

// DeserializeReplication A function to deserialize replication requests
func DeserializeReplication(serializedRequest string) ReplicationRequest {
	fields := strings.Fields(serializedRequest)
	requestID, err0 := strconv.Atoi(fields[1])
	clientID, err1 := strconv.Atoi(fields[2])
	sourceID, err3 := strconv.Atoi(fields[4])
	targetNodeID, err4 := strconv.Atoi(fields[5])

	if (err0 != nil) || (err1 != nil) || (err3 != nil) || (err4 != nil) {
		logger.LogMsg(LogSign, 0, "Replicate:Deserialize() Deserialization failed")
		return ReplicationRequest{}
	}

	requestObj := ReplicationRequest{
		ID:                 requestID,
		Type:               Type(fields[0]),
		ClientID:           clientID,
		FileName:           fields[3],
		SourceID:           sourceID,
		TargetNodeID:       targetNodeID,
		TargetNodeIP:       fields[6],
		TargetNodeBasePort: fields[7],
		TrackerPort:        fields[8],
	}

	return requestObj
}

// PrintReplication A function to print replication requests
func PrintReplication(request ReplicationRequest) {
	fmt.Println("Request info:")
	fmt.Println("  ID = ", request.ID)
	fmt.Println("  Type = ", string(request.Type))
	fmt.Println("  ClientID = ", request.ClientID)
	fmt.Println("  FileName = ", request.FileName)
	fmt.Println("  SoruceID = ", request.SourceID)
	fmt.Println("  TargetNodeID = ", request.TargetNodeID)
	fmt.Println("  TargetNodeIP = ", request.TargetNodeIP)
	fmt.Println("  TargetNodeBasePort = ", request.TargetNodeBasePort)
	fmt.Println("  TrackerPort = ", request.TrackerPort)
	fmt.Println()
}
