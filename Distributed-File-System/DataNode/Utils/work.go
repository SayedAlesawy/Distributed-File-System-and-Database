package datanode

import (
	client "Distributed-Video-Processing-Cluster/Client/ClientUtil"
	comm "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Comm"
	logger "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Log"
	"fmt"

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
			deserializedRequest := client.DeserializeRequest(serializedRequest)

			logMsg := fmt.Sprintf("Received [%s] request from client#%d", deserializedRequest.Type, deserializedRequest.ClientID)
			logger.LogMsg(LogSignDN, datanodeObj.id, logMsg)

			go datanodeObj.handleRequest(deserializedRequest)
		}
	}
}

func (datanodeObj *dataNode) handleRequest(request client.Request) {
	if request.Type == client.Download {
		//Call download request handler
	} else if request.Type == client.Upload {
		datanodeObj.uploadRequestHandler(request)
	} else if request.Type == client.Display {
		//Call display request handler
	}
}

func (datanodeObj *dataNode) uploadRequestHandler(request client.Request) {
	logger.LogMsg(LogSignDN, datanodeObj.id, "Upload Request Handler Started")

	datanodeObj.receiveDataFromClient(request)
}
