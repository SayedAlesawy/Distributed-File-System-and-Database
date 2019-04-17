package datanode

import (
	"fmt"
	"log"
	"time"

	"github.com/pebbe/zmq4"
)

// dataNode A struct to represent the basic structure of a Data Node
type dataNode struct {
	ip          string //The IP of the current machine
	port        string //The port to which the current will publish
	id          int    //A unique ID for the current machine
	trackerIP   string //The IP of the tracker machine
	trackerPort string //The port of the tracker machine
}

// dtHeartbeatNode A struct to represent a data node that sends heartbeat signals
//This struct extends the dataNode struct for added functionality
type dtHeartbeatNode struct {
	publisherSocket   *zmq4.Socket  //A publisher socket to which the machine publishes
	heartbeatInterval time.Duration //Defines the frequency at which the data node sends heartbeats
	dataNode                        //Provides the basic functionality of a data node
}

// NewDataNode A constructor function for the dataNode type
func NewDataNode(_ip string, _port string, _id int, _trackerIP string, _trackerPort string) dataNode {
	dataNodeObj := dataNode{
		ip:          _ip,
		port:        _port,
		id:          _id,
		trackerIP:   _trackerIP,
		trackerPort: _trackerPort,
	}

	return dataNodeObj
}

// NewDtHeartbeatNode A constructor function for the DtHeartbeatNode type
func NewDtHeartbeatNode(_dataNodeObj dataNode, _heartbeatInterval time.Duration) dtHeartbeatNode {
	dtHeartbeatNodeObj := dtHeartbeatNode{
		heartbeatInterval: _heartbeatInterval,
		dataNode:          _dataNodeObj,
	}

	return dtHeartbeatNodeObj
}

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

// SendHeartBeat A function to publish heartbeat signals
func (dtHeartbeatNodeObj dtHeartbeatNode) SendHeartBeat() {
	defer dtHeartbeatNodeObj.publisherSocket.Close()

	dtHeartbeatNodeObj.establishPublisherConnection()

	for range time.Tick(dtHeartbeatNodeObj.heartbeatInterval) {
		heartbeat := fmt.Sprintf("Heartbeat %d", dtHeartbeatNodeObj.dataNode.id)

		dtHeartbeatNodeObj.publisherSocket.Send(heartbeat, 0)
		log.Println("Sent", heartbeat)
	}
}

// SendIP A function to let the heartbeat data node send its IP:Port to the tracker machine
func (dtHeartbeatNodeObj dtHeartbeatNode) SendIP() {
	socket, _ := zmq4.NewSocket(zmq4.REQ)
	defer socket.Close()

	connectionString := "tcp://" + dtHeartbeatNodeObj.dataNode.trackerIP + ":" + dtHeartbeatNodeObj.dataNode.trackerPort

	socket.Connect(connectionString)

	machineIP := dtHeartbeatNodeObj.dataNode.ip + ":" + dtHeartbeatNodeObj.dataNode.port
	acknowledge := ""

	for acknowledge != "ACK" {
		log.Println("[Heartbeat Data Node]", "Sending IP")

		socket.Send(machineIP, 0)

		log.Println("[Heartbeat Data Node]", "Sent bla bla IP")

		acknowledge, _ = socket.Recv(0)

		if acknowledge != "ACK" {
			log.Printf("[Heartbeat Data Node #%d] Failed to connect to Tracker ... Trying again\n", dtHeartbeatNodeObj.dataNode.id)
		}
	}

	log.Printf("[Heartbeat Data Node #%d] Sucessfully connected to Tracker\n", dtHeartbeatNodeObj.dataNode.id)
}
