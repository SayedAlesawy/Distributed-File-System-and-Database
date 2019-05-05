package datanode

import (
	comm "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Comm"
	fileutils "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/File"
	logger "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Log"
	request "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Request"
	"fmt"
	"path/filepath"
	"strconv"
	"time"

	"github.com/pebbe/zmq4"
)

// establishPublisherConnection A function to establish a TCP connection for publishing heartbeats
func (dataNodeLauncherObj *dataNodeLauncher) establishPublisherConnection() {
	publisher, ok := comm.Init(zmq4.PUB, "")
	dataNodeLauncherObj.publisherSocket = publisher
	logger.LogFail(ok, LogSignL, dataNodeLauncherObj.id, "establishPublisherConnection(): Failed to acquire Publisher Socket")

	var connectionString = []string{comm.GetConnectionString(dataNodeLauncherObj.ip, dataNodeLauncherObj.heartbeatPort)}
	comm.Bind(dataNodeLauncherObj.publisherSocket, connectionString)
}

// SendHandshake A function the datanode launcher uses to send the IPs and the ID of all 3 processes (HB and normal DNs (client ports))
func (dataNodeLauncherObj dataNodeLauncher) SendHandshake(handshake string) {
	socket, ok := comm.Init(zmq4.REQ, "")
	defer socket.Close()
	logger.LogFail(ok, LogSignL, dataNodeLauncherObj.id, "SendHandshake(): Failed to acquire request Socket")

	var connectionString = []string{comm.GetConnectionString(dataNodeLauncherObj.trackerIP, dataNodeLauncherObj.trackerIPsPort)}
	comm.Connect(socket, connectionString)

	sendStatus := false

	for sendStatus != true {
		logger.LogMsg(LogSignL, dataNodeLauncherObj.id, "Sending handshake")

		sendStatus = comm.SendString(socket, handshake)
		logger.LogFail(sendStatus, LogSignL, dataNodeLauncherObj.id, "SendHandshake(): Failed to connect to Tracker ... Trying again")
	}

	logger.LogMsg(LogSignL, dataNodeLauncherObj.id, "Successfully connected to Tracker")
}

// sendReplicationRequest A function to send replication request to target machine [Timeout after 30 secs]
func (datanodeObj *dataNode) sendReplicationRequest(req request.ReplicationRequest) bool {
	socket, ok := comm.Init(zmq4.REQ, "")
	defer socket.Close()
	logger.LogFail(ok, LogSignDN, datanodeObj.id, "sendReplicationRequest(): Failed to acquire request Socket")

	var connectionString = []string{comm.GetConnectionString(req.TargetNodeIP, req.TargetNodeBasePort+"21")}
	comm.Connect(socket, connectionString)

	var status = false

	sendChan := make(chan bool, 1)
	go func() {
		status = comm.SendString(socket, request.SerializeReplication(req))
		sendChan <- status
	}()
	select {
	case <-sendChan:
	case <-time.After(30 * time.Second):
		logger.LogMsg(LogSignDN, datanodeObj.id, "Sending replication request timedout after 30 secs")
		return false
	}

	logger.LogFail(status, LogSignDN, datanodeObj.id, "sendReplicationRequest(): Failed to send RPQ to target")
	logger.LogSuccess(status, LogSignDN, datanodeObj.id, "Successfully sent RPQ to target")

	return status
}

// receiveChunkCount A function to recieve the chunk count of a file
func (datanodeObj *dataNode) receiveChunkCount(socket *zmq4.Socket) (int, bool) {
	var chunkCount string
	var ok = false

	recvChan := make(chan bool, 1)
	go func() {
		chunkCount, ok = comm.RecvString(socket)
		recvChan <- ok
	}()
	select {
	case <-recvChan:
	case <-time.After(30 * time.Second):
		logger.LogMsg(LogSignDN, datanodeObj.id, "Receiving chunk count timedout after 30 secs")
		return 0, false
	}

	logger.LogFail(ok, LogSignDN, datanodeObj.id, "receiveChunkCount(): Error receiving chunk count")

	ret, convErr := strconv.Atoi(chunkCount)
	logger.LogErr(convErr, LogSignDN, datanodeObj.id, "receiveChunkCount(): Error converting chunk count from string to int")

	logger.LogSuccess(ok, LogSignDN, datanodeObj.id, "Received chunk count")

	return ret, (ok && (convErr == nil))
}

// sendChunkCount A function to send the chunk count of a file
func (datanodeObj *dataNode) sendChunkCount(socket *zmq4.Socket, chunksCount int) bool {
	logger.LogMsg(LogSignDN, datanodeObj.id, "Sending chunk count to target")

	var status = false

	sendChan := make(chan bool, 1)
	go func() {
		status = comm.SendString(socket, strconv.Itoa(chunksCount))
		sendChan <- status
	}()
	select {
	case <-sendChan:
	case <-time.After(30 * time.Second):
		logger.LogMsg(LogSignDN, datanodeObj.id, "Sending RPQ chunk count timedout after 30 secs")
		return false
	}

	logger.LogFail(status, LogSignDN, datanodeObj.id, "sendChunkCount(): Failed to RPQ send chunk count to target")
	logger.LogSuccess(status, LogSignDN, datanodeObj.id, "Successfully sent RPQ chunk count to target")

	return status
}

// receiveChunk A function to recieve a chunk of data
func (datanodeObj *dataNode) receiveChunk(socket *zmq4.Socket, chunkID int) ([]byte, bool) {
	var chunk []byte
	var ok = false

	recvChan := make(chan bool, 1)
	go func() {
		chunk, ok = comm.RecvBytes(socket)
		recvChan <- ok
	}()
	select {
	case <-recvChan:
	case <-time.After(time.Minute):
		logger.LogMsg(LogSignDN, datanodeObj.id, "Receiving data chunk timedout after 1 min")
		return []byte{}, false
	}

	logger.LogFail(ok, LogSignDN, datanodeObj.id, "receiveChunk(): Error receiving chunk")

	logger.LogSuccess(ok, LogSignDN, datanodeObj.id, fmt.Sprintf("Received chunk %d", chunkID))

	return chunk, ok
}

// sendChunk A function to send a chunk of data
func (datanodeObj *dataNode) sendDataChunk(socket *zmq4.Socket, data []byte, chunkID int) bool {
	logger.LogMsg(LogSignDN, datanodeObj.id, fmt.Sprintf("Sending chunk #%d to target", chunkID))

	var status = false

	sendChan := make(chan bool, 1)
	go func() {
		status = comm.SendBytes(socket, data)
		sendChan <- status
	}()
	select {
	case <-sendChan:
	case <-time.After(time.Minute):
		logger.LogMsg(LogSignDN, datanodeObj.id, "Sending data chunk timedout after 1 min")
		return false
	}

	logger.LogFail(status, LogSignDN, datanodeObj.id, "sendChunk(): Failed to send chunk to target")
	logger.LogSuccess(status, LogSignDN, datanodeObj.id, fmt.Sprintf("Successfully sent chunk #%d to target", chunkID))

	return status
}

func (datanodeObj *dataNode) receiveData(fileName string, ip string, port string, clientID int, dir int) int {
	socket, ok := comm.Init(zmq4.REP, "")
	defer socket.Close()
	logger.LogFail(ok, LogSignDN, datanodeObj.id, "receiveDataFromClient(): Failed to acquire response Socket")

	var connectionString = []string{comm.GetConnectionString(ip, port)}
	if dir == 1 {
		comm.Connect(socket, connectionString)
	} else {
		comm.Bind(socket, connectionString)
	}

	directory := "Client" + strconv.Itoa(clientID)
	fileutils.CreateDirectory(directory)

	path := filepath.Join(directory, fileName)
	file := fileutils.CreateFile(path)
	defer file.Close()

	chunkCount, chunkCountStatus := datanodeObj.receiveChunkCount(socket)

	if chunkCountStatus == false {
		logger.LogMsg(LogSignDN, datanodeObj.id, "receiveDataFromClient(): chunkCount Abort!")
		return 0
	}

	for i := 0; i < chunkCount; i++ {
		chunk, chunkStatus := datanodeObj.receiveChunk(socket, i+1)

		if chunkStatus == false {
			logger.LogMsg(LogSignDN, datanodeObj.id, "receiveDataFromClient(): Data chunk Abort!")
			return 0
		}

		fileutils.WriteChunk(file, chunk)
	}

	logger.LogMsg(LogSignDN, datanodeObj.id, "File received")

	return chunkCount
}

// sendData A function to send Data to the target machine
func (datanodeObj *dataNode) sendData(fileName string, targetID int, targetIP string, targetPort string, clientID int) bool {
	socket, ok := comm.Init(zmq4.REQ, "")
	defer socket.Close()
	logger.LogFail(ok, LogSignDN, datanodeObj.id, "sendData(): Failed to acquire request Socket")

	var connectionString = []string{comm.GetConnectionString(targetIP, targetPort)}
	comm.Connect(socket, connectionString)

	directory := "Client" + strconv.Itoa(clientID)
	path := filepath.Join(directory, fileName)
	file := fileutils.OpenFile(path)
	defer file.Close()

	chunksCount := fileutils.GetChunksCount(path)

	//Send the chunksCount to the DataNode
	chunkCountStatus := datanodeObj.sendChunkCount(socket, chunksCount)

	if chunkCountStatus == false {
		logger.LogMsg(LogSignDN, datanodeObj.id, "sendData(): Abort!")
		return false
	}

	//Send the actual chunks of data
	for i := 0; i < chunksCount; i++ {
		chunk, size, done := fileutils.ReadChunk(file)

		if done == true {
			break
		}

		chunkStatus := datanodeObj.sendDataChunk(socket, chunk[:size], i+1)

		if chunkStatus == false {
			logger.LogMsg(LogSignDN, datanodeObj.id, "sendData(): Abort!")
			return false
		}
	}

	logger.LogMsg(LogSignDN, datanodeObj.id, fmt.Sprintf("Successfully replicated file to data node #%d", targetID))
	return true
}

// sendPieces A function to send a group of pieces to clients
func (datanodeObj *dataNode) sendPieces(req request.UploadRequest, start int, chunksCount int, clientID int) bool {
	socket, ok := comm.Init(zmq4.REQ, "")
	defer socket.Close()
	logger.LogFail(ok, LogSignDN, datanodeObj.id, "sendPieces(): Failed to acquire request Socket")

	var connectionString = []string{comm.GetConnectionString(req.ClientIP, req.ClientPort)}
	comm.Connect(socket, connectionString)

	directory := "Client" + strconv.Itoa(clientID)
	path := filepath.Join(directory, req.FileName)

	file := fileutils.OpenSeekFile(path, start)
	defer file.Close()

	for i := 0; i < chunksCount; i++ {
		chunk, size, done := fileutils.ReadChunk(file)

		if done == true {
			break
		}

		chunkStatus := datanodeObj.sendDataChunk(socket, chunk[:size], start+i)

		if chunkStatus == false {
			logger.LogMsg(LogSignDN, datanodeObj.id, "sendPieces(): Abort!")
			return false
		}
	}

	logger.LogMsg(LogSignDN, datanodeObj.id, fmt.Sprintf("Successfully sent pieces to client #%d", req.ClientID))
	return true
}

// sendCompletionNotifcation A function to notify the tracker of an action completion
func (datanodeObj *dataNode) sendCompletionNotifcation(req request.CompletionRequest) bool {
	socket, ok := comm.Init(zmq4.REQ, "")
	defer socket.Close()
	logger.LogFail(ok, LogSignDN, datanodeObj.id, "sendCompletionNotifcation(): Failed to acquire request Socket")

	var connectionString = []string{comm.GetConnectionString(datanodeObj.trackerIP, datanodeObj.trackerPorts[1])}
	comm.Connect(socket, connectionString)

	var status = false

	sendChan := make(chan bool, 1)
	go func() {
		status = comm.SendString(socket, request.SerializeCompletion(req))
		sendChan <- status
	}()
	select {
	case <-sendChan:
	case <-time.After(30 * time.Second):
		logger.LogMsg(LogSignDN, datanodeObj.id, "Sending Completion Notifcation timedout after 30 secs")
		return false
	}

	logger.LogFail(status, LogSignDN, datanodeObj.id, "sendCompletionNotifcation(): Failed to notify tracker of completion")
	logger.LogSuccess(status, LogSignDN, datanodeObj.id, "Successfully notified tracker of completion")

	return status
}

// notifyReplicationCompletion A function to notify tracker of replication completion
func (datanodeObj *dataNode) notifyReplicationCompletion(port string, msg string) bool {
	socket, ok := comm.Init(zmq4.REQ, "")
	defer socket.Close()
	logger.LogFail(ok, LogSignDN, datanodeObj.id, "notifyReplicationCompletion(): Failed to acquire request Socket")

	var connectionString = []string{comm.GetConnectionString(datanodeObj.trackerIP, port)}
	comm.Connect(socket, connectionString)

	var status = false

	sendChan := make(chan bool, 1)
	go func() {
		status = comm.SendString(socket, msg)
		sendChan <- status
	}()
	select {
	case <-sendChan:
	case <-time.After(30 * time.Second):
		logger.LogMsg(LogSignDN, datanodeObj.id, "Sending Replication completion notifcation timedout after 30 secs")
		return false
	}

	logger.LogFail(status, LogSignDN, datanodeObj.id, "notifyReplicationCompletion(): Failed to notify tracker of completion")
	logger.LogSuccess(status, LogSignDN, datanodeObj.id, "Successfully notified tracker of replication completion")

	return status
}
