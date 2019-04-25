package trackernode

import (
	client "Distributed-Video-Processing-Cluster/Client/ClientUtil"
	comm "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Comm"
	constants "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Constants"
	logger "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Log"
	"fmt"
	"log"
	"time"

	"github.com/pebbe/zmq4"
)

// ListenToClientRequests A function to listen to client requests
func (trackerNodeObj *trackerNode) ListenToClientRequests() {
	socket, ok := comm.Init(zmq4.REP, "")
	defer socket.Close()
	logger.LogFail(ok, LogSignTR, trackerNodeObj.id, "ListenToClientRequests(): Failed to acquire response Socket")

	var connectionString = []string{comm.GetConnectionString(trackerNodeObj.ip, trackerNodeObj.requestsPort)}
	comm.Bind(socket, connectionString)

	for {
		serializedRequest, recvStatus := comm.RecvString(socket)

		if recvStatus == true {
			deserializedRequest := client.DeserializeRequest(serializedRequest)

			logMsg := fmt.Sprintf("Received request from client#%d", deserializedRequest.ClientID)
			logger.LogMsg(LogSignTR, trackerNodeObj.id, logMsg)

			go trackerNodeObj.handleRequest(deserializedRequest)
		}
	}
}

// handleRequest A function to handle requests based on their types
func (trackerNodeObj *trackerNode) handleRequest(request client.Request) {
	if request.Type == client.Download {
		//Call download request handler
	} else if request.Type == client.Upload {
		trackerNodeObj.uploadRequestHandler(request)
	} else if request.Type == client.Display {
		//Call display request handler
	} else {
		log.Println(LogSignTR, "Invalid request type")
	}
}

// uploadRequestHandler A function to handle a request of type Upload
func (trackerNodeObj *trackerNode) uploadRequestHandler(request client.Request) {
	//Should check the DataNode database and choose an alive node
	//Until I install the DB, I will always assume that the first Data Node is always alive
	//And I will always pick it
	logger.LogMsg(LogSignTR, trackerNodeObj.id, "Upload Request Handler Started")

	dataNodeConnectionString := constants.TrackerResponse

	trackerNodeObj.sendDataNodePortsToClient(request, dataNodeConnectionString)
}

// downloadRequestHandler A function to handle requests of type download
func (trackerNodeObj *trackerNode) downloadRequestHandler(request client.Request) {
	logger.LogMsg(LogSignTR, trackerNodeObj.id, "Download Request Handler Started")

}

// Replicate A function that implements the periodic Replication routine
func (trackerNodeObj *trackerNode) Replicate() {
	// Do replication routine logic
	//1- Check DB for all files' instance counts
	//2- Run the replication routine for all files that match the critira
	id := 0
	clientID := 1
	fileName := "CA.mp4"
	sourceIP := constants.DataNodeLauncherIP
	targetIP := constants.DataNodeLauncherIP
	sourcePort := "7024"
	targetRPort := "6024"
	sourceID := 1
	targetNodeID := 2

	for range time.Tick(constants.ReplicationRoutineFrequency) {
		logger.LogMsg(LogSignL, trackerNodeObj.id, "Replication Routine, running ...")

		repReqObj := ReplicationRequest{
			ID:              id,
			ClientID:        clientID,
			FileName:        fileName,
			SourceID:        sourceID,
			TargetNodeID:    targetNodeID,
			TargetNodeIP:    targetIP,
			TargetNodeRPort: targetRPort,
		}

		trackerNodeObj.sendReplicationRequest(repReqObj, sourceIP, sourcePort)
	}
}
