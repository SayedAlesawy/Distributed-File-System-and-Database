package datanode

import (
	client "Distributed-Video-Processing-Cluster/Client/ClientUtil"
	comm "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Comm"
	fileutils "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/File"
	logger "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Log"
	"fmt"
	"strconv"

	"github.com/pebbe/zmq4"
)

// EstablishPublisherConnection A function to establish a TCP connection for publishing heartbeats
func (dataNodeLauncherObj *dataNodeLauncher) establishPublisherConnection() {
	publisher, ok := comm.Init(zmq4.PUB, "")
	dataNodeLauncherObj.publisherSocket = publisher
	logger.LogFail(ok, LogSignL, dataNodeLauncherObj.id, "establishPublisherConnection(): Failed to acquire Publisher Socket")

	var connectionString = []string{comm.GetConnectionString(dataNodeLauncherObj.dataNode.ip, dataNodeLauncherObj.heartbeatPort)}
	comm.Connect(dataNodeLauncherObj.publisherSocket, connectionString)

	comm.Bind(dataNodeLauncherObj.publisherSocket, connectionString)
}

// SendHandshake A function the datanode launcher uses to send the IPs and the ID of all 3 processes (HB and normal DNs (client ports))
func (dataNodeLauncherObj dataNodeLauncher) SendHandshake(handshake string) {
	socket, ok := comm.Init(zmq4.REQ, "")
	defer socket.Close()
	logger.LogFail(ok, LogSignL, dataNodeLauncherObj.id, "SendHandshake(): Failed to acquire request Socket")

	var connectionString = []string{comm.GetConnectionString(dataNodeLauncherObj.dataNode.trackerIP, dataNodeLauncherObj.trackerIPsPort)}
	comm.Connect(socket, connectionString)

	sendStatus := false

	for sendStatus != true {
		logger.LogMsg(LogSignL, dataNodeLauncherObj.dataNode.id, "Sending handshake")

		sendStatus = comm.SendString(socket, handshake)
		logger.LogFail(sendStatus, LogSignL, dataNodeLauncherObj.id, "SendHandshake(): Failed to connect to Tracker ... Trying again")
	}

	logger.LogMsg(LogSignL, dataNodeLauncherObj.dataNode.id, "Successfully connected to Tracker")
}

// receiveChunkCount A function to recieve the chunk count of a file
func (datanodeObj *dataNode) receiveChunkCount(socket *zmq4.Socket) (int, bool) {
	chunkCount, ok := comm.RecvString(socket)
	logger.LogFail(ok, LogSignDN, datanodeObj.id, "receiveChunkCount(): Error receiving chunk count")

	ret, convErr := strconv.Atoi(chunkCount)
	logger.LogErr(convErr, LogSignDN, datanodeObj.id, "receiveChunkCount(): Error converting chunk count from string to int")

	logger.LogSuccess(ok, LogSignDN, datanodeObj.id, "Received chunk count")

	return ret, (ok && (convErr == nil))
}

// receiveChunk A function to recieve a chunk of data
func (datanodeObj *dataNode) receiveChunk(socket *zmq4.Socket, chunkID int) ([]byte, bool) {
	chunk, ok := comm.RecvBytes(socket)
	logger.LogFail(ok, LogSignDN, datanodeObj.id, "receiveChunk(): Error receiving chunk")

	logger.LogSuccess(ok, LogSignDN, datanodeObj.id, fmt.Sprintf("Received chunk %d", chunkID))

	return chunk, ok
}

func (datanodeObj *dataNode) receiveDataFromClient(request client.Request) {
	socket, ok := comm.Init(zmq4.REP, "")
	defer socket.Close()
	logger.LogFail(ok, LogSignDN, datanodeObj.id, "receiveDataFromClient(): Failed to acquire response Socket")

	var connectionString = []string{comm.GetConnectionString(datanodeObj.ip, datanodeObj.port)}
	comm.Bind(socket, connectionString)

	file := fileutils.CreateFile(request.FileName)
	defer file.Close()

	chunkCount, chunkCountStatus := datanodeObj.receiveChunkCount(socket)

	if chunkCountStatus == false {
		logger.LogMsg(LogSignDN, datanodeObj.id, "receiveDataFromClient(): Abort!")
		return
	}

	for i := 0; i < chunkCount; i++ {
		chunk, chunkStatus := datanodeObj.receiveChunk(socket, i+1)

		if chunkStatus == false {
			logger.LogMsg(LogSignDN, datanodeObj.id, "receiveDataFromClient(): Abort!")
			return
		}

		fileutils.WriteChunk(file, chunk)
	}

	logger.LogMsg(LogSignDN, datanodeObj.id, "File received")
}
