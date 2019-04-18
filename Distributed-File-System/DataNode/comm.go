package datanode

import (
	"log"
	"strconv"

	"github.com/pebbe/zmq4"
)

// getPublisherSocket A function to obtain a publisher socket
func (dtHeartbeatNodeObj *dtHeartbeatNode) getPublisherSocket() {
	publisher, err := zmq4.NewSocket(zmq4.PUB)

	if err != nil {
		log.Printf("[Heartbeat Data Node #%d] Failed to acquire Publisher Socket\n", dtHeartbeatNodeObj.dataNode.id)
		return
	}

	dtHeartbeatNodeObj.publisherSocket = publisher
}

// EstablishPublisherConnection A function to establish a TCP connection for publishing heartbeats
func (dtHeartbeatNodeObj *dtHeartbeatNode) establishPublisherConnection() {
	dtHeartbeatNodeObj.getPublisherSocket()

	dtHeartbeatNodeObj.publisherSocket.SetLinger(0)

	connectionString := "tcp://" + dtHeartbeatNodeObj.dataNode.ip + ":" + dtHeartbeatNodeObj.dataNode.port

	dtHeartbeatNodeObj.publisherSocket.Bind(connectionString)
}

// SendIP A function to let the heartbeat data node send its IP:Port to the tracker machine
func (dtHeartbeatNodeObj dtHeartbeatNode) SendIP() {
	socket, _ := zmq4.NewSocket(zmq4.REQ)
	defer socket.Close()

	connectionString := "tcp://" + dtHeartbeatNodeObj.dataNode.trackerIP + ":" + dtHeartbeatNodeObj.dataNode.trackerPort

	socket.Connect(connectionString)

	machineIP := dtHeartbeatNodeObj.dataNode.ip + ":" + dtHeartbeatNodeObj.dataNode.port + " " + strconv.Itoa(dtHeartbeatNodeObj.dataNode.id)
	acknowledge := ""

	for acknowledge != "ACK" {
		log.Printf("[Heartbeat Data Node #%d] Sending IP\n", dtHeartbeatNodeObj.dataNode.id)

		socket.Send(machineIP, 0)

		acknowledge, _ = socket.Recv(0)

		if acknowledge != "ACK" {
			log.Printf("[Heartbeat Data Node #%d] Failed to connect to Tracker ... Trying again\n", dtHeartbeatNodeObj.dataNode.id)
		}
	}

	log.Printf("[Heartbeat Data Node #%d] Successfully connected to Tracker\n", dtHeartbeatNodeObj.dataNode.id)
}
