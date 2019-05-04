package main

import (
	client "Distributed-Video-Processing-Cluster/Client/ClientUtil"
	constants "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Constants"
	logger "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Log"
	request "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Request"
	"fmt"
	"os"
	"strconv"
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

		if requestType == "up" {
			logger.LogMsg(client.LogSign, clientID, "File name = ")
			fmt.Scanf("%s", &fileName)

			requestObj := request.UploadRequest{
				ID:         requestID,
				Type:       request.Upload,
				ClientID:   clientID,
				ClientIP:   clientIP,
				ClientPort: clientPort + "0",
				FileName:   fileName,
			}

			clientObj.UploadHandler(requestObj)

		} else if requestType == "dwn" {
			logger.LogMsg(client.LogSign, clientID, "File name = ")
			fmt.Scanf("%s", &fileName)

			requestObj := request.UploadRequest{
				ID:         requestID,
				Type:       request.Download,
				ClientID:   clientID,
				ClientIP:   clientIP,
				ClientPort: clientPort + "0",
				FileName:   fileName,
			}

			clientObj.DownloadHandler(requestObj)

		} else if requestType == "ls" {
			requestObj := request.UploadRequest{
				ID:         requestID,
				Type:       request.Display,
				ClientID:   clientID,
				ClientIP:   clientIP,
				ClientPort: clientPort + "0",
				FileName:   "dummy",
			}

			clientObj.DisplayHandler(requestObj)
		}

		requestID++
	}

	clientObj.CloseConnection()
}
