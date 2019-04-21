package datanode

import (
	client "Distributed-Video-Processing-Cluster/Client/ClientUtil"
	"log"

	"github.com/pebbe/zmq4"
)

// ListenToClients A function to listen to requests from clients
func (datanodeObj *dataNode) ListenToClients() {
	socket, _ := zmq4.NewSocket(zmq4.REP)
	defer socket.Close()

	connectionString := "tcp://" + datanodeObj.ip + ":" + datanodeObj.reqPort

	socket.Bind(connectionString)
	acknowledge := "ACK"

	for {
		serializedRequest, _ := socket.Recv(0)

		if serializedRequest != "" {
			deserializedRequest := client.DeserializeRequest(serializedRequest)

			socket.Send(acknowledge, 0)

			log.Println(LogSignDN, "#", datanodeObj.id, "Received request of type",
				deserializedRequest.Type, "from client#", deserializedRequest.ClientID)

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
	log.Println(LogSignDN, "#", datanodeObj.id, "Upload Request Handler Started")

	datanodeObj.receiveDataFromClient(request)
}
