package trackernode

import (
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pebbe/zmq4"
)

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

// disconnectSocket A function to disconnect a socket specified by an endpoint
func (heartbeatTrackerNodeObj *heartbeatTrackerNode) disconnectSocket(ip string) {
	heartbeatTrackerNodeObj.subscriberSocket.Disconnect(ip)
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
