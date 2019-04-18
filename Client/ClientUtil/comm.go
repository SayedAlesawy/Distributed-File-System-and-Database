package client

import (
	"log"

	"github.com/pebbe/zmq4"
)

// getSocket A function to obtain a comm socket
func (clientObj *client) getSocket() {
	socket, err := zmq4.NewSocket(zmq4.REQ)

	if err != nil {
		log.Printf("[Client #%d] Failed to acquire a Socket\n", clientObj.id)
		return
	}

	clientObj.socket = socket
}

// establishConnection A function to establish communication with all Tracker ports
func (clientObj *client) establishConnection() {
	clientObj.getSocket()

	for _, port := range clientObj.trackerPorts {
		connectionString := "tcp://" + clientObj.trackerIP + ":" + port

		clientObj.socket.Connect(connectionString)
	}
}

// SendRequest A function to send a request to Tracker
func (clientObj *client) SendRequest(request Request) {
	defer clientObj.socket.Close()

	clientObj.establishConnection()

	serializedRequest := serializeRequest(request)

	acknowledge := ""

	for acknowledge != "ACK" {
		log.Printf("[Client #%d] Sending Request\n", clientObj.id)

		clientObj.socket.Send(serializedRequest, 0)

		acknowledge, _ = clientObj.socket.Recv(0)

		if acknowledge != "ACK" {
			log.Printf("[Client #%d] Failed to send request to Tracker ... Trying again\n", clientObj.id)
		}
	}

	log.Printf("[Client #%d] Successfully sent request to Tracker\n", clientObj.id)
}
