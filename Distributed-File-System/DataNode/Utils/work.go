package datanode

import (
	comm "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Comm"
	logger "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Log"
	request "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Request"
	"strconv"
	"strings"
	"time"

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

	fileSize := datanodeObj.receiveData(req.FileName, req.ClientIP, req.ClientPort, req.ClientID, 1)
	if fileSize == 0 {
		return
	}

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

	status := datanodeObj.sendCompletionNotifcation(compReq)
	if status == false {
		return
	}
}

func (datanodeObj *dataNode) downloadRequestHandler(req request.UploadRequest, start int, chunksCount int) {
	logger.LogMsg(LogSignDN, datanodeObj.id, "Download Request Handler Started")

	sendStatus := datanodeObj.sendPieces(req, start, chunksCount, req.ClientID)
	if sendStatus == false {
		return
	}
}

func (datanodeObj *dataNode) replicationRequestHandler(req request.ReplicationRequest) {
	logger.LogMsg(LogSignDN, datanodeObj.id, "Replication Request Handler Started")

	if req.SourceID == datanodeObj.id {
		logger.LogMsg(LogSignDN, datanodeObj.id, "Replication Source")

		sendRPQStatus := datanodeObj.sendReplicationRequest(req)
		if sendRPQStatus == false {
			return
		}

		sendDataStatus := datanodeObj.sendData(req.FileName, req.TargetNodeID, req.TargetNodeIP, req.TargetNodeBasePort+"24", req.ClientID)
		if sendDataStatus == false {
			logger.LogMsg(LogSignDN, datanodeObj.id, "Replication Failed")

			notifyStatus := datanodeObj.notifyReplicationCompletion(req.TrackerPort, "Replication Failed")
			time.Sleep(5 * time.Second)
			if notifyStatus == false {
				return
			}

			return
		}

		notifyStatus := datanodeObj.notifyReplicationCompletion(req.TrackerPort, "Replication Finished")
		if notifyStatus == false {
			return
		}

	} else if req.TargetNodeID == datanodeObj.id {
		logger.LogMsg(LogSignDN, datanodeObj.id, "Replication Destination")

		recvStatus := datanodeObj.receiveData(req.FileName, datanodeObj.ip, datanodeObj.repUpPort, req.ClientID, 2)
		if recvStatus == 0 {
			return
		}

	} else {
		logger.LogMsg(LogSignDN, datanodeObj.id, "Malformed replication request")
	}
}
