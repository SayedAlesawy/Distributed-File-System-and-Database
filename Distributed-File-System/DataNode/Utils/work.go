package datanode

import (
	comm "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Comm"
	logger "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Log"
	request "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Request"

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
		//Call download request handler
	} else if reqType == request.Invalid {
		logger.LogMsg(LogSignDN, datanodeObj.id, "Invalid Request")
		return
	}
}

func (datanodeObj *dataNode) uploadRequestHandler(req request.UploadRequest) {
	logger.LogMsg(LogSignDN, datanodeObj.id, "Upload Request Handler Started")

	datanodeObj.receiveDataFromClient(req)
}
