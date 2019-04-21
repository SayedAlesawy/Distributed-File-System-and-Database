package trackernode

import (
	client "Distributed-Video-Processing-Cluster/Client/ClientUtil"
	"log"

	"github.com/pebbe/zmq4"
)

// ListenToClientRequests A function to listen to client requests
func (trackerNodeObj *trackerNode) ListenToClientRequests() {
	socket, _ := zmq4.NewSocket(zmq4.REP)
	defer socket.Close()

	connectionString := "tcp://" + trackerNodeObj.ip + ":" + trackerNodeObj.requestsPort

	socket.Bind(connectionString)
	acknowledge := "ACK"

	for {
		serializedRequest, _ := socket.Recv(0)

		if serializedRequest != "" {
			deserializedRequest := client.DeserializeRequest(serializedRequest)

			socket.Send(acknowledge, 0)

			log.Println(LogSignTR, "#", trackerNodeObj.id, "Received request from client#", deserializedRequest.ClientID)

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
