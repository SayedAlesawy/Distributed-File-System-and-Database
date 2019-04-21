package datanode

import (
	client "Distributed-Video-Processing-Cluster/Client/ClientUtil"
	"log"

	"github.com/pebbe/zmq4"
)

// getPublisherSocket A function to obtain a publisher socket
func (dataNodeLauncherObj *dataNodeLauncher) getPublisherSocket() {
	publisher, err := zmq4.NewSocket(zmq4.PUB)

	if err != nil {
		log.Println(LogSignL, dataNodeLauncherObj.dataNode.id, "Failed to acquire Publisher Socket")
		return
	}

	dataNodeLauncherObj.publisherSocket = publisher
}

// EstablishPublisherConnection A function to establish a TCP connection for publishing heartbeats
func (dataNodeLauncherObj *dataNodeLauncher) establishPublisherConnection() {
	dataNodeLauncherObj.getPublisherSocket()

	dataNodeLauncherObj.publisherSocket.SetLinger(0)

	connectionString := "tcp://" + dataNodeLauncherObj.dataNode.ip + ":" + dataNodeLauncherObj.heartbeatPort

	dataNodeLauncherObj.publisherSocket.Bind(connectionString)
}

// SendHandshake A function the datanode launcher uses to send the IPs and the ID of all 3 processes (HB and normal DNs (client ports))
func (dataNodeLauncherObj dataNodeLauncher) SendHandshake(handshake string) {
	socket, _ := zmq4.NewSocket(zmq4.REQ)
	defer socket.Close()

	connectionString := "tcp://" + dataNodeLauncherObj.dataNode.trackerIP + ":" + dataNodeLauncherObj.trackerIPsPort

	socket.Connect(connectionString)

	acknowledge := ""

	for acknowledge != "ACK" {
		log.Println(LogSignL, dataNodeLauncherObj.dataNode.id, "Sending handshake")

		socket.Send(handshake, 0)

		acknowledge, _ = socket.Recv(0)

		if acknowledge != "ACK" {
			log.Println(LogSignL, dataNodeLauncherObj.dataNode.id, "Failed to connect to Tracker ... Trying again")
		}
	}

	log.Println(LogSignL, dataNodeLauncherObj.dataNode.id, "Successfully connected to Tracker")
}

func (datanodeObj *dataNode) receiveDataFromClient(requst client.Request) {
	socket, _ := zmq4.NewSocket(zmq4.REP)
	defer socket.Close()

	connectionString := "tcp://" + datanodeObj.ip + ":" + datanodeObj.port

	socket.Bind(connectionString)
	acknowledge := "ACK"

	data, _ := socket.Recv(0)

	if data != "" {
		socket.Send(acknowledge, 0)

		log.Println(LogSignDN, "#", datanodeObj.id, "Received data")
		log.Println("Data:", data)
		log.Println("From client #", requst.ClientID)
	}

	log.Println(LogSignDN, "#", datanodeObj.id, "Finished serving request")
}
