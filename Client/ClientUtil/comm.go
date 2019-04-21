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

// EstablishConnection A function to establish communication with all Tracker ports
func (clientObj *client) EstablishConnection() {
	clientObj.getSocket()

	for _, port := range clientObj.trackerPorts {
		connectionString := "tcp://" + clientObj.trackerIP + ":" + port

		clientObj.socket.Connect(connectionString)

		log.Println("[Client]", "Connected to Tracker with port = ", port)
	}
}

// SendRequest A function to send a request to Tracker
func (clientObj *client) SendRequest(request Request) {
	serializedRequest := SerializeRequest(request)

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

// ReceiveResponse A function to receive the response sent by the Tracker
func (clientObj *client) ReceiveResponse() string {
	socket, _ := zmq4.NewSocket(zmq4.REP)
	defer socket.Close()

	connectionString := "tcp://" + clientObj.ip + ":" + clientObj.port

	socket.Bind(connectionString)
	acknowledge := "ACK"

	response, _ := socket.Recv(0)

	if response != "" {
		log.Printf("[Client #%d] Successfully received response from Tracker\n", clientObj.id)

		socket.Send(acknowledge, 0)

		return response
	}

	log.Printf("[Client #%d] Failed to receive response from Tracker\n", clientObj.id)

	return response
}

// RSendRequestToDN ..
func (clientObj *client) RSendRequestToDN(dnIP string, dnReqPort string, request Request) {
	socket, _ := zmq4.NewSocket(zmq4.REQ)
	defer socket.Close()

	connectionString := "tcp://" + dnIP + ":" + dnReqPort

	socket.Connect(connectionString)
	acknowledge := ""
	serializedRequest := SerializeRequest(request)

	log.Printf("[Client #%d] Resending request to DataNode\n", clientObj.id)

	socket.Send(serializedRequest, 0)

	acknowledge, _ = socket.Recv(0)

	if acknowledge != "ACK" {
		log.Printf("[Client #%d] Failed to re-send request to DataNode\n", clientObj.id)
	} else {
		log.Printf("[Client #%d] Successfully re-sent request to Data Node\n", clientObj.id)
	}
}

// SendData ..
func (clientObj *client) SendData(dnIP string, dnDataPort string) {
	socket, _ := zmq4.NewSocket(zmq4.REQ)
	defer socket.Close()

	connectionString := "tcp://" + dnIP + ":" + dnDataPort

	socket.Connect(connectionString)
	acknowledge := ""
	data := "Consider this is a very long piece of data but I am just so lazy to make it so"

	log.Printf("[Client #%d] Sending data to DataNode\n", clientObj.id)

	socket.Send(data, 0)

	acknowledge, _ = socket.Recv(0)

	if acknowledge != "ACK" {
		log.Printf("[Client #%d] Failed to send data to DataNode\n", clientObj.id)
	} else {
		log.Printf("[Client #%d] Successfully sent data to Data Node\n", clientObj.id)
	}
}

func (clientObj *client) CloseConnection() {
	clientObj.socket.Close()
}
