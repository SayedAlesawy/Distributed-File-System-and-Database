package trackernode

import (
	comm "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Comm"
	logger "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Log"
	request "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Request"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pebbe/zmq4"
)

// establishSubscriberConnection A function to establish a TCP connection for subscribing to heartbeats
func (trackerNodeLauncherObj *trackerNodeLauncher) establishSubscriberConnection() {
	subscriber, ok := comm.Init(zmq4.SUB, "Heartbeat")
	trackerNodeLauncherObj.subscriberSocket = subscriber
	logger.LogFail(ok, LogSignL, trackerNodeLauncherObj.id, "establishPublisherConnection(): Failed to acquire subscriber Socket")
}

// serializeIPsMaps A function to serialize IP maps
func (trackerNodeLauncherObj *trackerNodeLauncher) getHBConnections() []string {
	var connectionStrings []string

	trackerNodeLauncherObj.ipsMutex.Lock()
	trackerNodeLauncherObj.portsMutex.Lock()

	for id, ip := range trackerNodeLauncherObj.datanodeIPs {
		connection := comm.GetConnectionString(ip, trackerNodeLauncherObj.datanodeBasePorts[id]+"00")
		connectionStrings = append(connectionStrings, connection)
	}

	trackerNodeLauncherObj.portsMutex.Unlock()
	trackerNodeLauncherObj.ipsMutex.Unlock()

	return connectionStrings
}

// updateSubscriberConnection A function to update the heartbeat susbcription list
func (trackerNodeLauncherObj *trackerNodeLauncher) updateSubscriberConnection() {
	comm.Connect(trackerNodeLauncherObj.subscriberSocket, trackerNodeLauncherObj.getHBConnections())
}

// ReceiveHandshake A function to constantly check for incoming datanode handshakes
func (trackerNodeLauncherObj *trackerNodeLauncher) ReceiveHandshake() {
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
			incomingIP := fields[0]
			incomingBasePort := fields[2]
			incomingID, convErr := strconv.Atoi(fields[1])
			logger.LogErr(convErr, LogSignL, trackerNodeLauncherObj.id, "ReceiveHandshake(): Failed to convert incoming ID")

			trackerNodeLauncherObj.ipsMutex.Lock()
			trackerNodeLauncherObj.datanodeIPs[incomingID] = incomingIP
			trackerNodeLauncherObj.ipsMutex.Unlock()

			trackerNodeLauncherObj.portsMutex.Lock()
			trackerNodeLauncherObj.datanodeBasePorts[incomingID] = incomingBasePort
			trackerNodeLauncherObj.portsMutex.Unlock()

			trackerNodeLauncherObj.timeStampMutex.Lock()
			trackerNodeLauncherObj.datanodeTimeStamps[incomingID] = time.Now()
			trackerNodeLauncherObj.timeStampMutex.Unlock()

			trackerNodeLauncherObj.dbMutex.Lock()
			insertDataNode(trackerNodeLauncherObj.db, incomingID, incomingIP, incomingBasePort)
			trackerNodeLauncherObj.dbMutex.Unlock()

			logMsg := fmt.Sprintf("Received IP = %s from data node#%d", incomingIP+":"+incomingBasePort+"00", incomingID)
			logger.LogMsg(LogSignL, 0, logMsg)
		}
	}
}

// sendDataNodePortsToClient A function send a data node connection string to client
func (trackerNodeObj *trackerNode) sendDataNodePortsToClient(req request.UploadRequest, dataNodeConnectionString string) {
	socket, ok := comm.Init(zmq4.REQ, "")
	defer socket.Close()
	logger.LogFail(ok, LogSignTR, trackerNodeObj.id, "sendDataNodePortsToClient(): Failed acquire request socket")

	var connectionString = []string{comm.GetConnectionString(req.ClientIP, req.ClientPort)}
	comm.Connect(socket, connectionString)

	status := false

	for status != true {

		logger.LogMsg(LogSignTR, trackerNodeObj.id, fmt.Sprintf("Responding to request#%d, from client #%d", req.ID, req.ClientID))

		status = comm.SendString(socket, dataNodeConnectionString)
		logger.LogFail(status, LogSignTR, trackerNodeObj.id, fmt.Sprintf("sendDataNodePortsToClient(): Failed to respond to request#%d, from client #%d, ... Trying again",
			req.ID, req.ClientID))
	}

	logger.LogMsg(LogSignTR, trackerNodeObj.id, fmt.Sprintf("Responded to request#%d, from client #%d", req.ID, req.ClientID))
}

// sendReplicationRequest A function to send a replication request
func (trackerNodeObj *trackerNode) sendReplicationRequest(req request.ReplicationRequest, sourceIP string, sourcePort string) {
	socket, ok := comm.Init(zmq4.REQ, "")
	defer socket.Close()
	logger.LogFail(ok, LogSignTR, trackerNodeObj.id, "sendReplicationRequest(): Failed acquire request socket")

	var connectionString = []string{comm.GetConnectionString(sourceIP, sourcePort)}
	comm.Connect(socket, connectionString)

	logMsg := fmt.Sprintf("Sending RPQ {Src:%d, file:%s, client:%d, Dst:%d}",
		req.SourceID, req.FileName, req.ClientID, req.TargetNodeID)
	logger.LogMsg(LogSignTR, trackerNodeObj.id, logMsg)

	status := comm.SendString(socket, request.SerializeReplication(req))
	logger.LogFail(status, LogSignTR, trackerNodeObj.id, "sendDataNodePortsToClient(): Failed to send RPQ")
	logger.LogSuccess(status, LogSignTR, trackerNodeObj.id, "Successfully sent RPQ")
}

// notifyClient A function to notify client of an action compeltion
func (trackerNodeObj *trackerNode) notifyClient(ip string, port string, msg string, id int) {
	socket, ok := comm.Init(zmq4.REQ, "")
	defer socket.Close()
	logger.LogFail(ok, LogSignTR, trackerNodeObj.id, "notifyClient(): Failed acquire request socket")

	var connectionString = []string{comm.GetConnectionString(ip, port)}
	comm.Connect(socket, connectionString)

	logger.LogMsg(LogSignTR, trackerNodeObj.id, fmt.Sprintf("Sending notification to client %d", id))

	status := comm.SendString(socket, msg)
	logger.LogFail(status, LogSignTR, trackerNodeObj.id, "notifyClient(): Failed to send notification")
	logger.LogSuccess(status, LogSignTR, trackerNodeObj.id, "Successfully sent notification")
}

func (trackerNodeObj *trackerNode) recieveReplicationCompletion() bool {
	socket, ok := comm.Init(zmq4.REP, "")
	defer socket.Close()
	logger.LogFail(ok, LogSignTR, trackerNodeObj.id, "recieveReplicationCompletion(): Failed to acquire response Socket")

	var connectionString = []string{comm.GetConnectionString(trackerNodeObj.ip, trackerNodeObj.datanodePort)}
	comm.Bind(socket, connectionString)

	msg, status := comm.RecvString(socket)
	logger.LogFail(status, LogSignTR, trackerNodeObj.id, "recieveReplicationCompletion(): Failed to receive replication completion")
	logger.LogSuccess(status, LogSignTR, trackerNodeObj.id, "Recieved "+msg)

	return (msg == "Replication Finished")
}
