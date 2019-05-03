package main

import (
	client "Distributed-Video-Processing-Cluster/Client/ClientUtil"
	constants "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Constants"
	fileutils "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/File"
	request "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Request"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

func main() {
	//Tracker data
	trackerIP := constants.TrackerIP
	trackerPorts := constants.TrackerReqPorts

	//Client data
	clientIP := constants.ClientIP
	clientID, _ := strconv.Atoi(os.Args[1])
	clientPort := os.Args[2]

	clientObj := client.NewClient(clientID, clientIP, clientPort+"0", trackerIP, trackerPorts)
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
		if requestType == "up" {
			requestObj := request.UploadRequest{
				ID:         requestID,
				Type:       request.Upload,
				ClientID:   clientID,
				ClientIP:   clientIP,
				ClientPort: clientPort + "0",
				FileName:   fileName,
			}

			serializeRequest := request.SerializeUpload(requestObj)

			clientObj.SendRequest(serializeRequest)
			response := clientObj.ReceiveResponse()

			log.Println("[Client #]", clientID, "Received this:", response)

			arr := strings.Fields(response)

			clientObj.RSendRequestToDN(arr[0], arr[1], serializeRequest)

			clientObj.SendData(requestObj, arr[0], arr[1])
		} else if requestType == "dwn" {
			requestObj := request.UploadRequest{
				ID:         requestID,
				Type:       request.Download,
				ClientID:   clientID,
				ClientIP:   clientIP,
				ClientPort: clientPort + "0",
				FileName:   fileName,
			}

			serializeRequest := request.SerializeUpload(requestObj)
			clientObj.SendRequest(serializeRequest)
			response := clientObj.ReceiveResponse()
			log.Println("[Client #]", clientID, "Received this:", response)

			arr := strings.Fields(response)
			chunkCount, _ := strconv.Atoi(arr[0])
			dataNodeCount := (len(arr) - 1) / 2
			chunkEach := chunkCount / dataNodeCount
			start := 0
			blockID := 1
			currPort := 1

			done := make(chan bool)

			for i := 1; i < len(arr)-1; i += 2 {
				if i == len(arr)-2 {
					chunkEach += (chunkCount % dataNodeCount)
				}

				requestObj.ClientPort = requestObj.ClientPort[:3] + strconv.Itoa(i)
				serializeRequest := request.SerializeUpload(requestObj)

				req := serializeRequest + " " + strconv.Itoa(start) + " " + strconv.Itoa(chunkEach)

				start += chunkEach
				clientObj.RSendRequestToDN(arr[i], arr[i+1]+"1", req)

				go clientObj.RecvPieces(requestObj, strconv.Itoa(blockID), chunkEach, done)

				blockID++
				currPort++
			}

			for i := 0; i < dataNodeCount; i++ {
				log.Println("[Client #]", clientID, "Thread", <-done)
			}

			fileutils.AssembleFile(requestObj.FileName, requestObj.FileName[:len(requestObj.FileName)-4], requestObj.FileName[len(requestObj.FileName)-4:], dataNodeCount)
		}

		requestID++
	}

	clientObj.CloseConnection()
}
