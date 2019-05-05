package client

import (
	comm "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Comm"
	fileutils "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/File"
	logger "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Log"
	request "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Request"
	"fmt"
	"strconv"
	"time"

	"github.com/pebbe/zmq4"
)

// EstablishConnection A function to establish communication with all Tracker ports
func (clientObj *client) EstablishConnection() {
	socket, ok := comm.Init(zmq4.REQ, "")
	logger.LogFail(ok, LogSign, clientObj.id, "EstablishConnection(): Failed to acquire request Socket")

	clientObj.socket = socket

	var connectionString []string

	for _, port := range clientObj.trackerPorts {
		connectionString = append(connectionString, comm.GetConnectionString(clientObj.trackerIP, port))
	}

	comm.Connect(socket, connectionString)
	logger.LogMsg(LogSign, clientObj.id, "Successfully connected to tracker ports")
}

// SendRequest A function to send a request to Tracker [Timeout after 30 secs]
func (clientObj *client) SendRequest(serializedRequest string) bool {
	logger.LogMsg(LogSign, clientObj.id, "Sending Request to tracker")
	var sendStatus = false

	sendChan := make(chan bool, 1)
	go func() {
		sendStatus = comm.SendString(clientObj.socket, serializedRequest)
		sendChan <- sendStatus
	}()
	select {
	case <-sendChan:
	case <-time.After(30 * time.Second):
		logger.LogMsg(LogSign, clientObj.id, "Sending request to tracker timedout after 30 secs")
		return false
	}

	logger.LogFail(sendStatus, LogSign, clientObj.id, "SendRequest(): Failed to send request to tracker")

	return sendStatus
}

// ReceiveResponse A function to receive the response sent by the Tracker [Timeout after 30 secs]
func (clientObj *client) ReceiveResponse() (string, bool) {
	socket, ok := comm.Init(zmq4.REP, "")
	defer socket.Close()
	logger.LogFail(ok, LogSign, clientObj.id, "ReceiveResponse(): Failed to acquire response Socket")

	var connectionString = []string{comm.GetConnectionString(clientObj.ip, clientObj.port)}
	comm.Bind(socket, connectionString)

	var response string
	var status = false

	recvChan := make(chan bool, 1)
	go func() {
		response, status = comm.RecvString(socket)
		recvChan <- status
	}()
	select {
	case <-recvChan:
	case <-time.After(30 * time.Second):
		logger.LogMsg(LogSign, clientObj.id, "Receving response from tracker timedout after 30 secs")
		return "", false
	}

	logger.LogFail(status, LogSign, clientObj.id, "ReceiveResponse(): Failed to receive tracker response")

	return response, status
}

// RSendRequestToDN A function to resend a request to DataNode after receiving its data from Tracker [Timeout after 30 secs]
func (clientObj *client) RSendRequestToDN(dnIP string, dnReqPort string, serializedRequest string) bool {
	socket, ok := comm.Init(zmq4.REQ, "")
	defer socket.Close()
	logger.LogFail(ok, LogSign, clientObj.id, "RSendRequestToDN(): Failed to acquire request Socket")

	var connectionString = []string{comm.GetConnectionString(dnIP, dnReqPort)}
	comm.Connect(socket, connectionString)

	logger.LogMsg(LogSign, clientObj.id, "Sending Request to DataNode")

	var sendStatus = false

	sendChan := make(chan bool, 1)
	go func() {
		sendStatus = comm.SendString(socket, serializedRequest)
		sendChan <- sendStatus
	}()
	select {
	case <-sendChan:
	case <-time.After(30 * time.Second):
		logger.LogMsg(LogSign, clientObj.id, "Sending request to DataNode timedout after 30 secs")
		return false
	}

	logger.LogFail(sendStatus, LogSign, clientObj.id, "RSendRequestToDN(): Failed to send request to DataNode")

	return sendStatus
}

// sendChunkCount A function to send the chunk count of a file [Timeout after 30 secs]
func (clientObj *client) sendChunkCount(socket *zmq4.Socket, chunksCount int) bool {
	logger.LogMsg(LogSign, clientObj.id, "Sending chunk count to DataNode")

	var status = false

	sendChan := make(chan bool, 1)
	go func() {
		status = comm.SendString(socket, strconv.Itoa(chunksCount))
		sendChan <- status
	}()
	select {
	case <-sendChan:
	case <-time.After(30 * time.Second):
		logger.LogMsg(LogSign, clientObj.id, "Sending chunk count timedout after 30 secs")
		return false
	}

	logger.LogFail(status, LogSign, clientObj.id, "sendChunkCount(): Failed to send chunk count to DataNode")

	return status
}

// sendChunk A function to send a chunk of data [Timeout after 1 min]
func (clientObj *client) sendDataChunk(socket *zmq4.Socket, data []byte, chunkID int) bool {
	logger.LogMsg(LogSign, clientObj.id, fmt.Sprintf("Sending chunk #%d to DataNode", chunkID))

	var status = false

	sendChan := make(chan bool, 1)
	go func() {
		status = comm.SendBytes(socket, data)
		sendChan <- status
	}()
	select {
	case <-sendChan:
	case <-time.After(time.Minute):
		logger.LogMsg(LogSign, clientObj.id, "Sending chunk timedout after 1 min")
		return false
	}

	logger.LogFail(status, LogSign, clientObj.id, "sendChunk(): Failed to send chunk to DataNode")

	return status
}

// SendData A function to send data to DataNode chunk by chunk
func (clientObj *client) SendData(req request.UploadRequest, dnIP string, dnDataPort string) bool {
	socket, ok := comm.Init(zmq4.REQ, "")
	defer socket.Close()
	logger.LogFail(ok, LogSign, clientObj.id, "SendData(): Failed to acquire request Socket")

	var connectionString = []string{comm.GetConnectionString(clientObj.ip, clientObj.port)}
	comm.Bind(socket, connectionString)

	file := fileutils.OpenFile(req.FileName)
	defer file.Close()

	chunksCount := fileutils.GetChunksCount(req.FileName)

	//Send the chunksCount to the DataNode
	sendChunkCountStatus := clientObj.sendChunkCount(socket, chunksCount)
	logger.LogSuccess(sendChunkCountStatus, LogSign, clientObj.id, "Successfully sent chunk count to DataNode")
	if sendChunkCountStatus == false {
		return false
	}

	//Send the actual chunks of data
	for i := 0; i < chunksCount; i++ {
		chunk, size, done := fileutils.ReadChunk(file)

		if done == true {
			break
		}

		sendChunkStatus := clientObj.sendDataChunk(socket, chunk[:size], i+1)
		logger.LogSuccess(sendChunkStatus, LogSign, clientObj.id, fmt.Sprintf("Successfully sent chunk #%d", i+1))
		if sendChunkStatus == false {
			return false
		}
	}

	return true
}

// receiveChunk A function to receive a chunk of data [Timeout after 1 min]
func (clientObj *client) receiveChunk(socket *zmq4.Socket, chunkID int) ([]byte, bool) {
	var chunk []byte
	var status = false

	recvChan := make(chan bool, 1)
	go func() {
		chunk, status = comm.RecvBytes(socket)
		recvChan <- status
	}()
	select {
	case <-recvChan:
	case <-time.After(time.Minute):
		logger.LogMsg(LogSign, clientObj.id, "Receiving chunk from DataNode timedout after 1 min")
		return []byte{}, false
	}

	logger.LogFail(status, LogSign, clientObj.id, fmt.Sprintf("receiveChunk(): Error receiving chunk #%d", chunkID))

	return chunk, status
}

// RecvPieces A function to receive file pieces from DataNode
func (clientObj *client) RecvPieces(req request.UploadRequest, start string, chunkCount int, done chan bool) bool {
	socket, ok := comm.Init(zmq4.REP, "")
	defer socket.Close()
	logger.LogFail(ok, LogSign, clientObj.id, "RecvPieces(): Failed to acquire response Socket")

	var connectionString = []string{comm.GetConnectionString(clientObj.ip, req.ClientPort)}
	comm.Bind(socket, connectionString)

	file := fileutils.CreateFile(req.FileName[:len(req.FileName)-4] + "#" + start + req.FileName[len(req.FileName)-4:])
	defer file.Close()

	for i := 0; i < chunkCount; i++ {
		chunk, chunkStatus := clientObj.receiveChunk(socket, i+1)
		logger.LogSuccess(chunkStatus, LogSign, clientObj.id, fmt.Sprintf("Successfully received chunk #%d", i+1))
		if chunkStatus == false {
			done <- true
			return false
		}
		//TODO: Clean up
		fileutils.WriteChunk(file, chunk)
	}

	logger.LogMsg(LogSign, clientObj.id, fmt.Sprintf("Received Block #%s", start))

	done <- true
	return true
}

// ReceiveResponse A function to receive the response sent by the Tracker
func (clientObj *client) ReceiveNotification() (string, bool) {
	socket, ok := comm.Init(zmq4.REP, "")
	defer socket.Close()
	logger.LogFail(ok, LogSign, clientObj.id, "ReceiveNotification(): Failed to acquire response Socket")

	var connectionString = []string{comm.GetConnectionString(clientObj.ip, clientObj.notifyPort)}
	comm.Bind(socket, connectionString)

	var response string
	var sendStatus bool

	recieveNotificationChan := make(chan bool, 1)
	go func() {
		response, sendStatus = comm.RecvString(socket)
		recieveNotificationChan <- sendStatus
	}()
	select {
	case <-recieveNotificationChan:
	case <-time.After(30 * time.Second):
		logger.LogMsg(LogSign, clientObj.id, "ReceiveNotification(): Connection timed out after 30 secs")
		return "", false
	}

	logger.LogFail(sendStatus, LogSign, clientObj.id, "ReceiveNotification(): Failed to receive notification from tracker")

	return response, sendStatus
}

// CloseConnection A function to shut down the request port socket
func (clientObj *client) CloseConnection() {
	clientObj.socket.Close()
}
