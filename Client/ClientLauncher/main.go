package main

import (
	client "Distributed-Video-Processing-Cluster/Client/ClientUtil"
	"fmt"
	"log"
)

func main() {
	//Tracker data
	trackerIP := "127.0.0.1"
	trackerPorts := []string{"8001", "8002"}

	//Client data
	clientIP := "127.0.0.1"
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

		requestObj := client.Request{
			ID:         requestID,
			ClientID:   clientID,
			ClientIP:   clientIP,
			ClientPort: clientPort,
			FileName:   fileName,
			Type:       client.RequestType(requestType),
		}

		clientObj.SendRequest(requestObj)

		requestID++
	}

	clientObj.CloseConnection()
}
