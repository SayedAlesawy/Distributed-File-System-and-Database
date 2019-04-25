package trackernode

import (
	comm "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Comm"
	constants "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Constants"
	logger "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Log"
	request "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Request"
	"fmt"
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
			go trackerNodeObj.handleRequest(serializedRequest)
		}
	}
}

// handleRequest A function to handle requests based on their types
func (trackerNodeObj *trackerNode) handleRequest(serializedRequest string) {
	reqType := request.GetType(serializedRequest)

	if reqType == request.Upload {
		req := request.DeserializeUpload(serializedRequest)
		trackerNodeObj.uploadRequestHandler(req)
	} else if reqType == request.Download {
		//Call download request handler
	} else if reqType == request.Invalid {
		logger.LogMsg(LogSignTR, trackerNodeObj.id, "Invalid Request")
		return
	}
}

// uploadRequestHandler A function to handle a request of type Upload
func (trackerNodeObj *trackerNode) uploadRequestHandler(req request.UploadRequest) {
	//Should check the DataNode database and choose an alive node
	//Until I install the DB, I will always assume that the first Data Node is always alive
	//And I will always pick it
	logMsg := fmt.Sprintf("Handling upload request #%d, from client #%d", req.ID, req.ClientID)
	logger.LogMsg(LogSignTR, trackerNodeObj.id, logMsg)

	dataNodeConnectionString := constants.TrackerResponse

	trackerNodeObj.sendDataNodePortsToClient(req, dataNodeConnectionString)
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
	sourcePort := "7021"
	targetBasePort := "60"
	sourceID := 1
	targetNodeID := 2

	for range time.Tick(constants.ReplicationRoutineFrequency) {
		logger.LogMsg(LogSignL, trackerNodeObj.id, "Replication Routine, running ...")

		repReqObj := request.ReplicationRequest{
			ID:                 id,
			Type:               request.Replicate,
			ClientID:           clientID,
			FileName:           fileName,
			SourceID:           sourceID,
			TargetNodeID:       targetNodeID,
			TargetNodeIP:       targetIP,
			TargetNodeBasePort: targetBasePort,
		}

		trackerNodeObj.sendReplicationRequest(repReqObj, sourceIP, sourcePort)

		targetBasePort = "50"
		targetNodeID = 3
	}
}
