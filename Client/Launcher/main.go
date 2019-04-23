package main

import (
	client "Distributed-Video-Processing-Cluster/Client/ClientUtil"
	constants "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Constants"
	"fmt"
	"log"
	"strings"
)

func main() {
	//Tracker data
	trackerIP := constants.TrackerIP
	trackerPorts := constants.TrackerReqPorts

	//Client data
	clientIP := constants.ClientIP
	clientID := 1
	clientPort := ""

	//Read Client data
	log.Print("[Client] ID = ")
	fmt.Scanf("%d", &clientID)

	log.Print("[Client] Port = ")
	fmt.Scanf("%s", &clientPort)

	clientObj := client.NewClient(clientID, clientIP, clientPort, trackerIP, trackerPorts)
	clientObj.EstablishConnection()

	log.Println("[Client]", "Successfully launched")

	//Request data
	requestID := 1
	fileName := ""
	requestType := ""
	work := 1

	for {
		log.Println("[Client]", "Press 0 to quit, 1 to continue")
		fmt.Scanf("%d", &work)

		if work == 0 {
			break
		}

		log.Println("[Client]", "Request type = ")
		fmt.Scanf("%s", &requestType)

		log.Println("Client", "File name = ")
		fmt.Scanf("%s", &fileName)
		//TODO handle file not found before sending requests to datanode and maybe before sending
		//to tracker 7ata
		requestObj := client.Request{
			ID:         requestID,
			ClientID:   clientID,
			ClientIP:   clientIP,
			ClientPort: clientPort,
			FileName:   fileName,
			Type:       client.RequestType(requestType),
		}

		clientObj.SendRequest(requestObj)
		response := clientObj.ReceiveResponse()

		log.Println("[Client #]", clientID, "Received this:", response)

		arr := strings.Fields(response)

		clientObj.RSendRequestToDN(arr[0], arr[2], requestObj)

		clientObj.SendData(requestObj, arr[0], arr[1])

		requestID++
	}

	clientObj.CloseConnection()
}