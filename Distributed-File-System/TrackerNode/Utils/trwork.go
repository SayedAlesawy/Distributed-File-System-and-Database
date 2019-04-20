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

			client.PrintRequest(deserializedRequest)
		}
	}
}
