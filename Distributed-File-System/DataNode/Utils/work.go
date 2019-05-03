package datanode

import (
	comm "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Comm"
	logger "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Log"
	request "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Request"
	"log"
	"strconv"
	"strings"

	"github.com/pebbe/zmq4"
)

// ListenToClients A function to listen to requests from clients
func (datanodeObj *dataNode) ListenToClients() {
	socket, ok := comm.Init(zmq4.REP, "")
	defer socket.Close()
	logger.LogFail(ok, LogSignDN, datanodeObj.id, "ListenToClients(): Failed to acquire response socket")

	connectionString := []string{comm.GetConnectionString(datanodeObj.ip, datanodeObj.reqPort)}
	comm.Bind(socket, connectionString)

	for {
		serializedRequest, recvStatus := comm.RecvString(socket)

		if recvStatus == true {
			go datanodeObj.handleRequest(serializedRequest)
		}
	}
}

func (datanodeObj *dataNode) handleRequest(serializedRequest string) {
	reqType := request.GetType(serializedRequest)

	if reqType == request.Upload {
		req := request.DeserializeUpload(serializedRequest)
		datanodeObj.uploadRequestHandler(req)

	} else if reqType == request.Download {
		req := request.DeserializeUpload(serializedRequest)
		arr := strings.Fields(serializedRequest)
		start, _ := strconv.Atoi(arr[6])
		chunkCount, _ := strconv.Atoi(arr[7])
		datanodeObj.downloadRequestHandler(req, start, chunkCount)

	} else if reqType == request.Replicate {
		req := request.DeserializeReplication(serializedRequest)
		datanodeObj.replicationRequestHandler(req)

	} else if reqType == request.Invalid {
		logger.LogMsg(LogSignDN, datanodeObj.id, "Invalid Request")
		return
	}
}

func (datanodeObj *dataNode) uploadRequestHandler(req request.UploadRequest) {
	logger.LogMsg(LogSignDN, datanodeObj.id, "Upload Request Handler Started")

	fileSize := datanodeObj.receiveData(req.FileName, req.ClientIP, req.ClientPort, 1)
	location := strconv.Itoa(datanodeObj.id)

	compReq := request.CompletionRequest{
		Type:       request.Completion,
		ClientID:   req.ClientID,
		ClientIP:   req.ClientIP,
		ClientPort: req.ClientPort,
		FileName:   req.FileName,
		FileSize:   fileSize,
		Location:   location,
	}

	datanodeObj.sendCompletionNotifcation(compReq)
}

func (datanodeObj *dataNode) downloadRequestHandler(req request.UploadRequest, start int, chunksCount int) {
	logger.LogMsg(LogSignDN, datanodeObj.id, "Download Request Handler Started")
	datanodeObj.sendPieces(req, start, chunksCount)
}

func (datanodeObj *dataNode) replicationRequestHandler(req request.ReplicationRequest) {
	logger.LogMsg(LogSignDN, datanodeObj.id, "Replication Request Handler Started")

	if req.SourceID == datanodeObj.id {
		log.Println("I am source")
		datanodeObj.sendReplicationRequest(req)
		datanodeObj.sendData(req.FileName, req.TargetNodeID, req.TargetNodeIP, req.TargetNodeBasePort+"24")
	} else if req.TargetNodeID == datanodeObj.id {
		log.Println("I am dest")
		datanodeObj.receiveData(req.FileName, datanodeObj.ip, datanodeObj.repUpPort, 2)
	} else {
		logger.LogMsg(LogSignDN, datanodeObj.id, "Malformed replication request")
	}
}
