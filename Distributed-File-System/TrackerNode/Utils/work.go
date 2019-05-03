package trackernode

import (
	comm "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Comm"
	constants "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Constants"
	logger "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Log"
	request "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Request"
	"fmt"
	"log"
	"strconv"
	"strings"
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

func (trackerNodeObj *trackerNode) getReplicationSrc(locations []int) (dataNodeRow, bool) {
	for i := 0; i < len(locations); i++ {
		trackerNodeObj.dbMutex.Lock()
		src, srcAlive := selectDataNode(trackerNodeObj.db, locations[i])
		trackerNodeObj.dbMutex.Unlock()

		if srcAlive == true {
			return src, true
		}
	}

	return dataNodeRow{}, false
}

func (trackerNodeObj *trackerNode) replicateFile(metafile fileRow) {
	fields := strings.Fields(metafile.location)

	//The file is fully replicated
	if len(fields) == 3 {
		return
	}

	var locations []int
	for i := 0; i < len(fields); i++ {
		id, _ := strconv.Atoi(fields[i])
		locations = append(locations, id)
	}

	newLocation := metafile.location

	//The file needs replication
	for dn := 1; dn <= 3; dn++ {
		found := false
		for i := 0; i < len(locations); i++ {
			if dn == locations[i] {
				found = true
				break
			}
		}
		//This dn has the file, no need to replicate
		if found == true {
			continue
		}

		//This dn doesn't have the file, we need to replicate

		//Get the destination node, and check if it's alive
		trackerNodeObj.dbMutex.Lock()
		dst, dstAlive := selectDataNode(trackerNodeObj.db, dn)
		trackerNodeObj.dbMutex.Unlock()

		if dstAlive == false { //If dst isn't alive, then ignore it
			log.Println("Dst is dead")
			continue
		}

		//Dst is alive, let's find a src
		src, srcAlive := trackerNodeObj.getReplicationSrc(locations)
		if srcAlive == false { //If there are no alive sources, then ignore it
			log.Println("Src is dead")
			continue
		}

		//We have an alive src and an alive dst
		//If any of them went down later, then no replication will take place
		//And the error handling will take care of that
		repReqObj := request.ReplicationRequest{
			ID:                 0,
			Type:               request.Replicate,
			ClientID:           metafile.clientID,
			FileName:           metafile.fileName,
			SourceID:           src.id,
			TargetNodeID:       dst.id,
			TargetNodeIP:       dst.ip,
			TargetNodeBasePort: dst.basePort,
			TrackerPort:        trackerNodeObj.datanodePort,
		}

		trackerNodeObj.sendReplicationRequest(repReqObj, src.ip, src.basePort+"21")
		success := trackerNodeObj.recieveReplicationCompletion()

		if success == true {
			newLocation += " " + strconv.Itoa(dst.id)
		}
	}

	//Update the metafile entry
	trackerNodeObj.dbMutex.Lock()
	updateMetaFile(trackerNodeObj.db, newLocation, metafile.fileName, metafile.clientID)
	trackerNodeObj.dbMutex.Unlock()
}

// Replicate A function that implements the periodic Replication routine
func (trackerNodeObj *trackerNode) Replicate() {
	for range time.Tick(constants.ReplicationRoutineFrequency) {
		logger.LogMsg(LogSignL, trackerNodeObj.id, "Replication Routine, running ...")

		trackerNodeObj.dbMutex.Lock()
		metaFiles := selectMetaFiles(trackerNodeObj.db)
		trackerNodeObj.dbMutex.Unlock()

		for _, metaFile := range metaFiles {
			trackerNodeObj.replicateFile(metaFile)
		}
	}
}

// completionRequestHandler A function to handle the completion notifications
func (trackerNodeObj *trackerNode) completionRequestHandler(req request.CompletionRequest) {
	trackerNodeObj.dbMutex.Lock()
	insertMetaFile(trackerNodeObj.db, req.FileName, req.ClientID, req.FileSize, req.Location)
	trackerNodeObj.dbMutex.Unlock()

	msg := fmt.Sprintf("Successfully uploaded file %s of size %d", req.FileName, req.FileSize)
	trackerNodeObj.notifyClient(req.ClientIP, req.ClientPort[:3]+"7", msg, req.ClientID)
}
