package trackernode

import (
	client "Distributed-Video-Processing-Cluster/Client/ClientUtil"
	"log"
	"strconv"
	"strings"
	"sync"

	"github.com/pebbe/zmq4"
)

// ListenToClientRequests A function to listen to client requests
func (trackerNodeObj *trackerNode) ListenToClientRequests() {
	socket, _ := zmq4.NewSocket(zmq4.REP)
	defer socket.Close()

	ip := trackerNodeObj.ip
	port := trackerNodeObj.requestsPort
	connectionString := "tcp://" + ip + ":" + port

	socket.Bind(connectionString)
	acknowledge := "ACK"

	for {
		serializedRequest, _ := socket.Recv(0)

		if serializedRequest != "" {
			deserializedRequest := client.DeserializeRequest(serializedRequest)

			socket.Send(acknowledge, 0)

			log.Println("[Tracker Node #]", trackerNodeObj.id, "Received request from client#", deserializedRequest.ClientID)
			client.PrintRequest(deserializedRequest)
		}
	}
}

// RecieveDataNodeIPs A function to cnstantly scan for incomding IPs of data nodes
func (trackerNodeObj *trackerNode) RecieveDataNodeIPs(IPsMutex *sync.Mutex) {
	socket, _ := zmq4.NewSocket(zmq4.REP)
	defer socket.Close()

	ip := trackerNodeObj.ip
	port := trackerNodeObj.datanodePort
	connectionString := "tcp://" + ip + ":" + port

	socket.Bind(connectionString)
	acknowledge := "ACK"

	for {
		msg, _ := socket.Recv(0)

		if msg != "" {
			fields := strings.Fields(msg)
			incomingIP := fields[0]
			incomingID, _ := strconv.Atoi(fields[1])

			IPsMutex.Lock()
			trackerNodeObj.datanodeIPs[incomingID] = incomingIP
			IPsMutex.Unlock()

			socket.Send(acknowledge, 0)

			log.Println("[Tracker Node]", "Received IP = ", incomingIP, "form node #", incomingID)
		}
	}
}
