package datanode

import (
	"fmt"
	"log"
	"time"

	"github.com/pebbe/zmq4"
)

// DataNode A struct to represent the data nodes
type dataNode struct {
	ip                string        //The IP of the current machine
	port              string        //The port to which the data node will publish
	id                int           //A unique ID for the data node
	publisherSocket   *zmq4.Socket  //A publisher socket to which the machine publishes
	heartbeatInterval time.Duration //Defines the frequency at which the data node sends heartbeats
}

// New A function to return a new instance of a Data Node
func New(_ip string, _port string, _id int, _heartbeatInterval time.Duration) dataNode {
	newDataNode := dataNode{ip: _ip, port: _port, id: _id, heartbeatInterval: _heartbeatInterval}

	return newDataNode
}

// getPublisherSocket A function to obtain a publisher socket
func (dataNodeObj *dataNode) getPublisherSocket() {
	publisher, err := zmq4.NewSocket(zmq4.PUB)

	if err != nil {
		fmt.Printf("[Data Node #%d] Failed to acquire Publisher Socket\n", dataNodeObj.id)
		return
	}
	//dataNodeObj.Port = "12525"
	dataNodeObj.publisherSocket = publisher
}

// EstablishPublisherConnection A function to establish a TCP connection for publishing heartbeats
func (dataNodeObj *dataNode) establishPublisherConnection() {
	dataNodeObj.getPublisherSocket()

	dataNodeObj.publisherSocket.SetLinger(0)

	connectionString := "tcp://" + dataNodeObj.ip + ":" + dataNodeObj.port

	dataNodeObj.publisherSocket.Bind(connectionString)
}

// SendHeartBeat A function to publish heartbeat signals
func (dataNodeObj dataNode) SendHeartBeat() {
	defer dataNodeObj.publisherSocket.Close()

	dataNodeObj.establishPublisherConnection()

	for range time.Tick(dataNodeObj.heartbeatInterval) {
		dataNodeObj.publisherSocket.Send("Heartbeat", 0)
		log.Println("Sent", "Heartbeat")
	}
}
