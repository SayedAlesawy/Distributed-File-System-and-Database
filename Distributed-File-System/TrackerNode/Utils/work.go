package trackernode

import (
	comm "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Comm"
	constants "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Constants"
	logger "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Log"
	request "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Request"
	"fmt"
	"strconv"
	"time"

	"github.com/pebbe/zmq4"
)

var lastPickedNode int
var lastPickedProcess = 1

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
		req := request.DeserializeUpload(serializedRequest)
		trackerNodeObj.downloadRequestHandler(req)

	} else if reqType == request.Completion {
		req := request.DeserializeCompletion(serializedRequest)

		trackerNodeObj.completionRequestHandler(req)

	} else if reqType == request.Invalid {
		logger.LogMsg(LogSignTR, trackerNodeObj.id, "Invalid Request")
		return
	}
}

// pickUploadDataNode A function pick a datanode to handle an upload request
func (trackerNodeObj *trackerNode) pickUploadDataNode() (dataNodeRow, int) {
	trackerNodeObj.dbMutex.Lock()
	res := selectDatanodes(trackerNodeObj.db)

	if lastPickedNode >= len(res) {
		lastPickedNode = 0
	}

	pickedDN := lastPickedNode
	lastPickedNode++

	if lastPickedProcess == 3 {
		lastPickedProcess = 1
	}

	pickedProcess := lastPickedProcess
	lastPickedProcess++
	trackerNodeObj.dbMutex.Unlock()

	return res[pickedDN], pickedProcess
}

// uploadRequestHandler A function to handle a request of type Upload
func (trackerNodeObj *trackerNode) uploadRequestHandler(req request.UploadRequest) {
	logMsg := fmt.Sprintf("Handling upload request #%d, from client #%d", req.ID, req.ClientID)
	logger.LogMsg(LogSignTR, trackerNodeObj.id, logMsg)

	pickedDN, pickedProcess := trackerNodeObj.pickUploadDataNode()

	dataNodeConnectionString := pickedDN.ip + " " + pickedDN.basePort + strconv.Itoa(pickedProcess) + "1"

	trackerNodeObj.sendDataNodePortsToClient(req, dataNodeConnectionString)
}

func (trackerNodeObj *trackerNode) downloadRequestHandler(req request.UploadRequest) {
	//Should do DB logic here
	logMsg := fmt.Sprintf("Handling download request #%d, from client #%d", req.ID, req.ClientID)
	logger.LogMsg(LogSignTR, trackerNodeObj.id, logMsg)

	chunksCount := "2887" //hardcoded for now, should be fetched from the DB

	downloadPorts := chunksCount + " " +
		constants.DownloadIP1 + " " + constants.DownloadPort1 + " " +
		constants.DownloadIP2 + " " + constants.DownloadPort2 + " " +
		constants.DownloadIP3 + " " + constants.DownloadPort3 + " " +
		constants.DownloadIP4 + " " + constants.DownloadPort4 + " " +
		constants.DownloadIP5 + " " + constants.DownloadPort5 + " " +
		constants.DownloadIP6 + " " + constants.DownloadPort6

	trackerNodeObj.sendDataNodePortsToClient(req, downloadPorts)
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

// completionRequestHandler A function to handle the completion notifications
func (trackerNodeObj *trackerNode) completionRequestHandler(req request.CompletionRequest) {
	trackerNodeObj.dbMutex.Lock()
	insertMetaFile(trackerNodeObj.db, req.FileName, req.ClientID, req.FileSize, req.Location)
	trackerNodeObj.dbMutex.Unlock()

	//msg := fmt.Sprintf("Successfully uploaded file %s of size %d", req.FileName, req.FileSize)
	//trackerNodeObj.notifyClient(req.ClientIP, req.ClientPort, msg, req.ClientID)
}
