package datanode

import (
	"log"
	"strconv"

	"github.com/pebbe/zmq4"
)

// sendDataNodeIP A function to let the data node send its IP:Port to the tracker machine
func (datanodeObj dataNode) sendDataNodeIP(connectionString string) {
	socket, _ := zmq4.NewSocket(zmq4.REQ)
	defer socket.Close()

	socket.Connect(connectionString)

	machineIP := datanodeObj.ip + ":" + datanodeObj.port + " " + strconv.Itoa(datanodeObj.id)
	acknowledge := ""

	for acknowledge != "ACK" {
		log.Printf("[Data Node #%d] Sending IP\n", datanodeObj.id)

		socket.Send(machineIP, 0)

		acknowledge, _ = socket.Recv(0)

		if acknowledge != "ACK" {
			log.Printf("[Data Node #%d] Failed to connect to Tracker ... Trying again\n", datanodeObj.id)
		}
	}

	log.Printf("[Data Node #%d] Successfully connected to Tracker\n", datanodeObj.id)
}

func (datanodeObj dataNode) SendDataNodeIPs() {
	for _, port := range datanodeObj.trackerPorts {
		connectionString := "tcp://" + datanodeObj.trackerIP + ":" + port

		datanodeObj.sendDataNodeIP(connectionString)
	}
}
