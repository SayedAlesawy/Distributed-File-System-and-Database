package trackernode

import (
	client "Distributed-Video-Processing-Cluster/Client/ClientUtil"
	comm "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Comm"
	logger "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Log"
	"fmt"
	"log"

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
			log.Println(LogSignTR, trackerNodeObj.id, logMsg)

			go trackerNodeObj.handleRequest(deserializedRequest)
		}
	}
}

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

func (trackerNodeObj *trackerNode) uploadRequestHandler(request client.Request) {
	//Should check the DataNode database and choose an alive node
	//Until I install the DB, I will always assume that the first Data Node is always alive
	//And I will always pick it
	log.Println(LogSignTR, "#", trackerNodeObj.id, "Upload Request Handler Started")

	dataNodeConnectionString := "127.0.0.1" + " " + "7001" + " " + "7003"

	trackerNodeObj.sendDataNodePortsToClient(request, dataNodeConnectionString)
}
