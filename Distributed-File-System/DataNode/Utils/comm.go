package datanode

import (
	client "Distributed-Video-Processing-Cluster/Client/ClientUtil"
	fileutils "Distributed-Video-Processing-Cluster/Distributed-File-System/FileUtils"
	"log"
	"strconv"

	"github.com/pebbe/zmq4"
)

// getPublisherSocket A function to obtain a publisher socket
func (dataNodeLauncherObj *dataNodeLauncher) getPublisherSocket() {
	publisher, err := zmq4.NewSocket(zmq4.PUB)

	if err != nil {
		log.Println(LogSignL, dataNodeLauncherObj.dataNode.id, "Failed to acquire Publisher Socket")
		return
	}

	dataNodeLauncherObj.publisherSocket = publisher
}

// EstablishPublisherConnection A function to establish a TCP connection for publishing heartbeats
func (dataNodeLauncherObj *dataNodeLauncher) establishPublisherConnection() {
	dataNodeLauncherObj.getPublisherSocket()

	dataNodeLauncherObj.publisherSocket.SetLinger(0)

	connectionString := "tcp://" + dataNodeLauncherObj.dataNode.ip + ":" + dataNodeLauncherObj.heartbeatPort

	dataNodeLauncherObj.publisherSocket.Bind(connectionString)
}

// SendHandshake A function the datanode launcher uses to send the IPs and the ID of all 3 processes (HB and normal DNs (client ports))
func (dataNodeLauncherObj dataNodeLauncher) SendHandshake(handshake string) {
	socket, _ := zmq4.NewSocket(zmq4.REQ)
	defer socket.Close()

	connectionString := "tcp://" + dataNodeLauncherObj.dataNode.trackerIP + ":" + dataNodeLauncherObj.trackerIPsPort

	socket.Connect(connectionString)

	acknowledge := ""

	for acknowledge != "ACK" {
		log.Println(LogSignL, dataNodeLauncherObj.dataNode.id, "Sending handshake")

		socket.Send(handshake, 0)

		acknowledge, _ = socket.Recv(0)

		if acknowledge != "ACK" {
			log.Println(LogSignL, dataNodeLauncherObj.dataNode.id, "Failed to connect to Tracker ... Trying again")
		}
	}

	log.Println(LogSignL, dataNodeLauncherObj.dataNode.id, "Successfully connected to Tracker")
}

// receiveChunkCount A function to recieve the chunk count of a file
func (datanodeObj *dataNode) receiveChunkCount(socket *zmq4.Socket) (int, bool) {
	chunkCount, _ := socket.Recv(0)
	acknowledge := "ACK"

	socket.Send(acknowledge, 0)

	ret, _ := strconv.Atoi(chunkCount)

	if chunkCount != "" {
		log.Println(LogSignDN, "#", datanodeObj.id, "Received chunk count")

		return ret, true
	}

	return ret, false
}

// receiveChunk A function to recieve a chunk of data
func (datanodeObj *dataNode) receiveChunk(socket *zmq4.Socket) ([]byte, bool) {
	chunk, err := socket.RecvBytes(0)
	acknowledge := "ACK"

	socket.Send(acknowledge, 0)

	if err == nil {
		log.Println(LogSignDN, "#", datanodeObj.id, "Received chunk")

		return chunk, true
	}

	return chunk, false
}

func (datanodeObj *dataNode) receiveDataFromClient(request client.Request) {
	socket, _ := zmq4.NewSocket(zmq4.REP)
	defer socket.Close()

	connectionString := "tcp://" + datanodeObj.ip + ":" + datanodeObj.port
	socket.Bind(connectionString)

	file := fileutils.CreateFile(request.FileName)
	defer file.Close()

	//Receive the chunks count
	chunkCount, _ := datanodeObj.receiveChunkCount(socket)

	for i := 0; i < chunkCount; i++ {
		chunk, _ := datanodeObj.receiveChunk(socket)

		fileutils.WriteChunk(file, chunk)
	}

	log.Println(LogSignDN, "#", datanodeObj.id, "Finished serving request")
}
