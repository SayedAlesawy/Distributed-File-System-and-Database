package main

import (
	client "Distributed-Video-Processing-Cluster/Client/ClientUtil"
	constants "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Constants"
	fileutils "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/File"
	logger "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Log"
	request "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Request"
	"fmt"
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
	clientNotifyPort := os.Args[2] + "7"

	clientObj := client.NewClient(clientID, clientIP, clientPort+"0", clientNotifyPort, trackerIP, trackerPorts)
	clientObj.EstablishConnection()

	logger.LogMsg(client.LogSign, clientID, "Successfully launched")

	//Request data
	requestID := 1
	fileName := ""
	requestType := ""
	work := 1

	for {
		logger.LogMsg(client.LogSign, clientID, "Press 0 to quit, 1 to continue")
		fmt.Scanf("%d", &work)

		if work == 0 {
			break
		}

		logger.LogMsg(client.LogSign, clientID, "Request type = ")
		fmt.Scanf("%s", &requestType)

		logger.LogMsg(client.LogSign, clientID, "File name = ")
		fmt.Scanf("%s", &fileName)

		if requestType == "up" {
			exists := fileutils.IsThere(fileName)
			if exists == false {
				logger.LogMsg(client.LogSign, clientID, fmt.Sprintf("%s :no such file or directory", fileName))
				continue
			}

			requestObj := request.UploadRequest{
				ID:         requestID,
				Type:       request.Upload,
				ClientID:   clientID,
				ClientIP:   clientIP,
				ClientPort: clientPort + "0",
				FileName:   fileName,
			}

			serializeRequest := request.SerializeUpload(requestObj)

			sendRequestStatus := clientObj.SendRequest(serializeRequest)
			logger.LogSuccess(sendRequestStatus, client.LogSign, clientID, "Successfully sent request to tracker")
			if sendRequestStatus == false {
				continue
			}

			response, receiveResponseStatus := clientObj.ReceiveResponse()
			logger.LogSuccess(receiveResponseStatus, client.LogSign, clientID, fmt.Sprintf("Tracker response: %s", response))
			if receiveResponseStatus == false {
				continue
			}

			if response == "All data nodes are offline" {
				continue
			}

			arr := strings.Fields(response)

			rsendRequestStatus := clientObj.RSendRequestToDN(arr[0], arr[1], serializeRequest)
			logger.LogSuccess(rsendRequestStatus, client.LogSign, clientID, "Successfully sent request to DataNode")
			if rsendRequestStatus == false {
				continue
			}

			sendDataStatus := clientObj.SendData(requestObj, arr[0], arr[1])
			logger.LogSuccess(sendDataStatus, client.LogSign, clientID, "Successfully sent file to DataNode")
			if sendDataStatus == false {
				continue
			}

			notification, notificationStatus := clientObj.ReceiveNotification()
			logger.LogSuccess(notificationStatus, client.LogSign, clientID, fmt.Sprintf("Tracker confirmation: %s", notification))
			if notificationStatus == false {
				continue
			}

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

			sendRequestStatus := clientObj.SendRequest(serializeRequest)
			logger.LogSuccess(sendRequestStatus, client.LogSign, clientID, "Successfully sent request to tracker")
			if sendRequestStatus == false {
				continue
			}

			response, receiveResponseStatus := clientObj.ReceiveResponse()
			logger.LogSuccess(receiveResponseStatus, client.LogSign, clientID, fmt.Sprintf("Tracker response: %s", response))
			if receiveResponseStatus == false {
				continue
			}

			if response == "404: File not found" {
				continue
			}

			if response == "All source datanodes are offline, try again later" {
				continue
			}

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

				rsendRequestStatus := clientObj.RSendRequestToDN(arr[i], arr[i+1]+"1", req)
				logger.LogSuccess(rsendRequestStatus, client.LogSign, clientID, "Successfully sent request to DataNode")
				if rsendRequestStatus == false {
					continue
				}

				go clientObj.RecvPieces(requestObj, strconv.Itoa(blockID), chunkEach, done)

				blockID++
				currPort++
			}

			for i := 0; i < dataNodeCount; i++ {
				logger.LogMsg(client.LogSign, clientID, fmt.Sprintf("Thread #%d finished %t", i+1, <-done))
			}

			fileutils.AssembleFile(requestObj.FileName, requestObj.FileName[:len(requestObj.FileName)-4], requestObj.FileName[len(requestObj.FileName)-4:], dataNodeCount)
		}

		requestID++
	}

	clientObj.CloseConnection()
}
