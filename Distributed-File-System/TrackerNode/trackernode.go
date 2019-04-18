package trackernode

import (
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

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
	subscriberSocket   *zmq4.Socket //A susbscriber socket
	datanodeIPs        map[int]string
	datanodeTimeStamps map[int]time.Time
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

	heartbeatTrackerNodeObj.datanodeIPs = make(map[int]string)
	heartbeatTrackerNodeObj.datanodeTimeStamps = make(map[int]time.Time)

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
func (heartbeatTrackerNodeObj *heartbeatTrackerNode) updateSubscriberConnection(IPsMutex *sync.Mutex) {
	IPsMutex.Lock()

	for _, ip := range heartbeatTrackerNodeObj.datanodeIPs {
		connectionString := "tcp://" + ip

		heartbeatTrackerNodeObj.subscriberSocket.Connect(connectionString)
	}

	IPsMutex.Unlock()
}

// ListenToHeartbeats A function to listen to incoming heartbeats
func (heartbeatTrackerNodeObj *heartbeatTrackerNode) ListenToHeartbeats(IPsMutex *sync.Mutex, timeStampsMutex *sync.Mutex) {
	heartbeatTrackerNodeObj.establishSubscriberConnection()

	defer heartbeatTrackerNodeObj.subscriberSocket.Close()

	for {
		heartbeatTrackerNodeObj.updateSubscriberConnection(IPsMutex)

		heartbeat, _ := heartbeatTrackerNodeObj.subscriberSocket.Recv(zmq4.DONTWAIT)

		if heartbeat != "" {
			log.Println("Received", heartbeat)

			heartbeatTrackerNodeObj.registerTimeStap(heartbeat, timeStampsMutex)
		}

		//heartbeatTrackerNodeObj.printMap(m)
	}
}

// registerTimeStap A function to register the timestamp of the last received heartbeat
func (heartbeatTrackerNodeObj *heartbeatTrackerNode) registerTimeStap(heartbeat string, timeStampMutex *sync.Mutex) {
	id, _ := strconv.Atoi((strings.Fields(heartbeat))[1])

	timeStampMutex.Lock()
	heartbeatTrackerNodeObj.datanodeTimeStamps[id] = time.Now()
	timeStampMutex.Unlock()
}

// updateDataNodeAliveStatus A function the update the status of the alive datanodes
func (heartbeatTrackerNodeObj *heartbeatTrackerNode) UpdateDataNodeAliveStatus(IPsMutex *sync.Mutex, timeStampsMutex *sync.Mutex) {
	for {
		timeStampsMutex.Lock()

		for id, timestamp := range heartbeatTrackerNodeObj.datanodeTimeStamps {
			diff := time.Now().Sub(timestamp)
			threshold := time.Duration(2000000001)

			if diff > threshold {
				log.Println("deleting node# ", id)
				IPsMutex.Lock()
				delete(heartbeatTrackerNodeObj.datanodeIPs, id)
				IPsMutex.Unlock()

				delete(heartbeatTrackerNodeObj.datanodeTimeStamps, id)
			}
		}

		timeStampsMutex.Unlock()
	}
}

func (heartbeatTrackerNodeObj heartbeatTrackerNode) printMap(IPsMutex *sync.Mutex) {
	IPsMutex.Lock()
	for id, ip := range heartbeatTrackerNodeObj.datanodeIPs {
		log.Println("Tracking: ", id, " -- ", ip)
	}
	IPsMutex.Unlock()
}

// ScanIPs A function to cnstantly scan for incomding IPs of data heartbeat nodes
func (heartbeatTrackerNodeObj *heartbeatTrackerNode) RecieveIP(IPsMutex *sync.Mutex, timeStampsMutex *sync.Mutex) {
	socket, _ := zmq4.NewSocket(zmq4.REP)
	defer socket.Close()

	ip := heartbeatTrackerNodeObj.trackerNode.ip
	port := heartbeatTrackerNodeObj.trackerNode.port
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
			heartbeatTrackerNodeObj.datanodeIPs[incomingID] = incomingIP
			IPsMutex.Unlock()

			timeStampsMutex.Lock()
			heartbeatTrackerNodeObj.datanodeTimeStamps[incomingID] = time.Now()
			timeStampsMutex.Unlock()

			socket.Send(acknowledge, 0)

			log.Println("[Heartbeat Tracker Node]", "Received IP = ", incomingIP, "form node #", incomingID)
		}
	}
}
