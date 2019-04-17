package trackernode

import (
	"log"

	"github.com/pebbe/zmq4"
)

// trackerNode A struct to represent the basic structure of a Tracker Node
type trackerNode struct {
	ip   string //The IP of the Tracker machine
	port string //The port of the Tracker machine
}

// heartbeatTrackerNode A struct to represent a Tracker Node that listens to heartbeats
//This struct extends the dataNode struct for added functionality
type heartbeatTrackerNode struct {
	subscriberSocket    *zmq4.Socket
	dtHeartbeatNodesIPs []string
	trackerNode
}

//NewTrackerNode A constructor function for the trackerNode type
func NewTrackerNode(_ip string, _port string) trackerNode {
	trackerNodeObj := trackerNode{
		ip:   _ip,
		port: _port,
	}

	return trackerNodeObj
}

// NewHeartbeatTrackerNode A constructor function for the heartbeatTrackerNode type
func NewHeartbeatTrackerNode(_trackerNodeObj trackerNode) heartbeatTrackerNode {
	heartbeatTrackerNodeObj := heartbeatTrackerNode{
		trackerNode: _trackerNodeObj,
	}

	return heartbeatTrackerNodeObj
}

// getSubscriberSocket A function to obtain a susbscriber socket
func (heartbeatTrackerNodeObj *heartbeatTrackerNode) getSubscriberSocket() {
	subscriber, err := zmq4.NewSocket(zmq4.SUB)

	if err != nil {
		log.Printf("[Heartbeat Tracker Node] Failed to acquire Subscriber Socket\n")
		return
	}

	heartbeatTrackerNodeObj.subscriberSocket = subscriber
}

// establishSubscriberConnection A function to establish a TCP connection for subscribing to heartbeats
func (heartbeatTrackerNodeObj *heartbeatTrackerNode) establishSubscriberConnection() {
	heartbeatTrackerNodeObj.getSubscriberSocket()

	heartbeatTrackerNodeObj.subscriberSocket.SetLinger(0)

	heartbeatTrackerNodeObj.subscriberSocket.SetSubscribe("Heartbeat")
}

// updateSubscriberConnection A function to update the heartbeat susbcription list
func (heartbeatTrackerNodeObj *heartbeatTrackerNode) updateSubscriberConnection() {
	//log.Println("[Heartbeat Tracker Node]", "Called IP update routine")

	for i := 0; i < len(heartbeatTrackerNodeObj.dtHeartbeatNodesIPs); i++ {
		connectionString := "tcp://" + heartbeatTrackerNodeObj.dtHeartbeatNodesIPs[i]
		//log.Println("Update", connectionString)
		heartbeatTrackerNodeObj.subscriberSocket.Connect(connectionString)
	}
}

// ListenToHeartbeats A function to listen to incoming heartbeats
func (heartbeatTrackerNodeObj *heartbeatTrackerNode) ListenToHeartbeats() {
	heartbeatTrackerNodeObj.establishSubscriberConnection()

	defer heartbeatTrackerNodeObj.subscriberSocket.Close()

	for {
		//log.Println("[Heartbeat Tracker Node]", "Waiting for heartbeats")
		heartbeatTrackerNodeObj.RecieveIP()

		heartbeatTrackerNodeObj.updateSubscriberConnection()

		heartbeat, _ := heartbeatTrackerNodeObj.subscriberSocket.Recv(0)

		if heartbeat != "" {
			log.Println("Received", heartbeat)
		}
	}
}

// RecieveIP A function to receive the IP of the heartbeat data node
func (heartbeatTrackerNodeObj *heartbeatTrackerNode) RecieveIP() {
	socket, _ := zmq4.NewSocket(zmq4.REP)
	defer socket.Close()

	ip := heartbeatTrackerNodeObj.trackerNode.ip
	port := heartbeatTrackerNodeObj.trackerNode.port
	connectionString := "tcp://" + ip + ":" + port

	socket.Bind(connectionString)
	acknowledge := "ACK"

	incomingIP, _ := socket.Recv(0)

	if incomingIP != "" {
		heartbeatTrackerNodeObj.dtHeartbeatNodesIPs = append(heartbeatTrackerNodeObj.dtHeartbeatNodesIPs, incomingIP)

		socket.Send(acknowledge, 0)

		log.Println("[Heartbeat Tracker Node]", "Received IP = ", incomingIP)
	}
}
