package trackernode

import (
	client "Distributed-Video-Processing-Cluster/Client/ClientUtil"
	comm "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Comm"
	logger "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Log"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pebbe/zmq4"
)

// establishSubscriberConnection A function to establish a TCP connection for subscribing to heartbeats
func (trackerNodeLauncherObj *trackerNodeLauncher) establishSubscriberConnection() {
	subscriber, ok := comm.Init(zmq4.SUB, "Heartbeat")
	trackerNodeLauncherObj.subscriberSocket = subscriber
	logger.LogFail(ok, LogSignL, trackerNodeLauncherObj.id, "establishPublisherConnection(): Failed to acquire subscriber Socket")
}

// updateSubscriberConnection A function to update the heartbeat susbcription list
func (trackerNodeLauncherObj *trackerNodeLauncher) updateSubscriberConnection(portsMutex *sync.Mutex) {
	comm.Connect(trackerNodeLauncherObj.subscriberSocket, getHBConnections(trackerNodeLauncherObj.datanodeBasePorts, portsMutex))
}

// ReceiveHandshake A function to constantly check for incoming datanode handshakes
func (trackerNodeLauncherObj *trackerNodeLauncher) ReceiveHandshake(portsMutex *sync.Mutex, timeStampsMutex *sync.Mutex) {
	socket, ok := comm.Init(zmq4.REP, "")
	defer socket.Close()
	logger.LogFail(ok, LogSignL, trackerNodeLauncherObj.id, "ReceiveHandshake(): Failed to acquire response Socket")

	var connectionString = []string{comm.GetConnectionString(trackerNodeLauncherObj.ip, trackerNodeLauncherObj.trackerIPsPort)}
	comm.Bind(socket, connectionString)

	for {
		msg, status := comm.RecvString(socket)
		logger.LogFail(status, LogSignL, trackerNodeLauncherObj.id, "ReceiveHandshake(): Failed to receive handshake")

		if status == true {
			fields := strings.Fields(msg)
			incomingBaseIP := fields[0] + ":" + fields[2]
			incomingID, convErr := strconv.Atoi(fields[1])
			logger.LogErr(convErr, LogSignL, trackerNodeLauncherObj.id, "ReceiveHandshake(): Failed to convert incoming ID")

			portsMutex.Lock()
			trackerNodeLauncherObj.datanodeBasePorts[incomingID] = incomingBaseIP
			portsMutex.Unlock()

			timeStampsMutex.Lock()
			trackerNodeLauncherObj.datanodeTimeStamps[incomingID] = time.Now()
			timeStampsMutex.Unlock()

			logMsg := fmt.Sprintf("Received IP = %s from data node#%d", incomingBaseIP+"00", incomingID)
			logger.LogMsg(LogSignL, 0, logMsg)
		}
	}
}

// sendDataNodePortsToClient A function send a data node connection string to client
func (trackerNodeObj *trackerNode) sendDataNodePortsToClient(request client.Request, dataNodeConnectionString string) {
	socket, ok := comm.Init(zmq4.REQ, "")
	defer socket.Close()
	logger.LogFail(ok, LogSignTR, trackerNodeObj.id, "sendDataNodePortsToClient(): Failed acquire request socket")

	var connectionString = []string{comm.GetConnectionString(request.ClientIP, request.ClientPort)}
	comm.Connect(socket, connectionString)

	status := false

	for status != true {

		logger.LogMsg(LogSignTR, trackerNodeObj.id, fmt.Sprintf("Responding to request#%d, from client #%d", request.ID, request.ClientID))

		status = comm.SendString(socket, dataNodeConnectionString)
		logger.LogFail(status, LogSignTR, trackerNodeObj.id, fmt.Sprintf("sendDataNodePortsToClient(): Failed to respond to request#%d, from client #%d, ... Trying again",
			request.ID, request.ClientID))
	}

	logger.LogMsg(LogSignTR, trackerNodeObj.id, fmt.Sprintf("Responded to request#%d, from client #%d", request.ID, request.ClientID))
}

func (trackerNodeObj *trackerNode) sendReplicationRequest(request ReplicationRequest, sourceIP string, sourcePort string) {
	socket, ok := comm.Init(zmq4.REQ, "")
	defer socket.Close()
	logger.LogFail(ok, LogSignTR, trackerNodeObj.id, "sendReplicationRequest(): Failed acquire request socket")

	var connectionString = []string{comm.GetConnectionString(sourceIP, sourcePort)}
	comm.Connect(socket, connectionString)

	logMsg := fmt.Sprintf("Sending RPQ {Src:%d, file:%s, client:%d, Dst:%d}",
		request.SourceID, request.FileName, request.ClientID, request.TargetNodeID)
	logger.LogMsg(LogSignTR, trackerNodeObj.id, logMsg)

	_, err := socket.Send(SerializeRequest(request), zmq4.DONTWAIT)
	status := true
	if err != nil {
		status = false
	}
	logger.LogFail(status, LogSignTR, trackerNodeObj.id, "sendDataNodePortsToClient(): Failed to send RPQ")
	logger.LogSuccess(status, LogSignTR, trackerNodeObj.id, "Successfully sent RPQ")
}

// serializeIPsMaps A function to serialize IP maps
func getHBConnections(ipsMap map[int]string, Mutex *sync.Mutex) []string {
	var connectionStrings []string

	Mutex.Lock()

	for _, ip := range ipsMap {
		connection := "tcp://" + ip + "00"
		connectionStrings = append(connectionStrings, connection)
	}

	Mutex.Unlock()

	return connectionStrings
}
