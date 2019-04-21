package trackernode

import (
	client "Distributed-Video-Processing-Cluster/Client/ClientUtil"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pebbe/zmq4"
)

// getSubscriberSocket A function to obtain a susbscriber socket
func (trackerNodeLauncherObj *trackerNodeLauncher) getSubscriberSocket() {
	subscriber, err := zmq4.NewSocket(zmq4.SUB)

	if err != nil {
		log.Printf("[Heartbeat Tracker Node] Failed to acquire Subscriber Socket\n")
		return
	}

	trackerNodeLauncherObj.subscriberSocket = subscriber
}

// establishSubscriberConnection A function to establish a TCP connection for subscribing to heartbeats
func (trackerNodeLauncherObj *trackerNodeLauncher) establishSubscriberConnection() {
	trackerNodeLauncherObj.getSubscriberSocket()

	trackerNodeLauncherObj.subscriberSocket.SetLinger(0)

	trackerNodeLauncherObj.subscriberSocket.SetSubscribe("Heartbeat")
}

// updateSubscriberConnection A function to update the heartbeat susbcription list
func (trackerNodeLauncherObj *trackerNodeLauncher) updateSubscriberConnection(HBIPsMutex *sync.Mutex) {
	HBIPsMutex.Lock()

	for _, ip := range trackerNodeLauncherObj.datanodeHBIPs {
		connectionString := "tcp://" + ip

		trackerNodeLauncherObj.subscriberSocket.Connect(connectionString)
	}

	HBIPsMutex.Unlock()
}

// disconnectSocket A function to disconnect a socket specified by an endpoint
func (trackerNodeLauncherObj *trackerNodeLauncher) disconnectSocket(ip string) {
	trackerNodeLauncherObj.subscriberSocket.Disconnect("tcp://" + ip)
}

// ReceiveHandshake A function to constantly check for incoming datanode handshakes
func (trackerNodeLauncherObj *trackerNodeLauncher) ReceiveHandshake(HBIPsMutex *sync.Mutex, DNIPsMutex *sync.Mutex, timeStampsMutex *sync.Mutex) {
	socket, _ := zmq4.NewSocket(zmq4.REP)
	defer socket.Close()

	connectionString := "tcp://" + trackerNodeLauncherObj.trackerNode.ip + ":" + trackerNodeLauncherObj.trackerIPsPort

	socket.Bind(connectionString)
	acknowledge := "ACK"

	for {
		msg, _ := socket.Recv(0)

		if msg != "" {
			fields := strings.Fields(msg)
			incomingHBIP := fields[0]
			incomdingDNIPs := pairIPs{fields[1], fields[2]}
			incomingID, _ := strconv.Atoi(fields[3])

			HBIPsMutex.Lock()
			trackerNodeLauncherObj.datanodeHBIPs[incomingID] = incomingHBIP
			HBIPsMutex.Unlock()

			DNIPsMutex.Lock()
			trackerNodeLauncherObj.datanodeIPs[incomingID] = incomdingDNIPs
			DNIPsMutex.Unlock()

			timeStampsMutex.Lock()
			trackerNodeLauncherObj.datanodeTimeStamps[incomingID] = time.Now()
			timeStampsMutex.Unlock()

			socket.Send(acknowledge, 0)

			log.Println(LogSignL, "Received IP = ", incomingHBIP, "form node #", incomingID)
		}
	}
}

// sendDataNodePortsToClient A function send a data node connection string to client
func (trackerNodeObj *trackerNode) sendDataNodePortsToClient(request client.Request, dataNodeConnectionString string) {
	socket, _ := zmq4.NewSocket(zmq4.REQ)
	defer socket.Close()

	clientConnectionString := "tcp://" + request.ClientIP + ":" + request.ClientPort

	socket.Connect(clientConnectionString)

	acknowledge := ""

	for acknowledge != "ACK" {
		log.Println(LogSignTR, trackerNodeObj.id, "Responding to request#", request.ID, "from Client #", request.ClientID)

		socket.Send(dataNodeConnectionString, 0)

		acknowledge, _ = socket.Recv(0)

		if acknowledge != "ACK" {
			log.Println(LogSignTR, trackerNodeObj.id, "Failed to respond to request#", request.ID, "from Client #", request.ClientID)
		}
	}

	log.Println(LogSignTR, trackerNodeObj.id, "Responded to request#", request.ID, "from Client #", request.ClientID)
}
