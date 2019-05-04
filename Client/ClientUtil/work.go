package client

import (
	fileutils "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/File"
	logger "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Log"
	request "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Request"
	"strconv"
	"strings"

	"fmt"
)

// UploadHandler A function to handle a upload request
func (clientObj *client) UploadHandler(req request.UploadRequest) {
	exists := fileutils.IsThere(req.FileName)
	if exists == false {
		logger.LogMsg(LogSign, clientObj.id, fmt.Sprintf("%s :no such file or directory", req.FileName))
		return
	}

	serializeRequest := request.SerializeUpload(req)

	sendRequestStatus := clientObj.SendRequest(serializeRequest)
	logger.LogSuccess(sendRequestStatus, LogSign, clientObj.id, "Successfully sent request to tracker")
	if sendRequestStatus == false {
		return
	}

	response, receiveResponseStatus := clientObj.ReceiveResponse()
	logger.LogSuccess(receiveResponseStatus, LogSign, clientObj.id, fmt.Sprintf("Tracker response: %s", response))
	if receiveResponseStatus == false {
		return
	}

	if response == "All data nodes are offline" {
		return
	}

	arr := strings.Fields(response)

	rsendRequestStatus := clientObj.RSendRequestToDN(arr[0], arr[1], serializeRequest)
	logger.LogSuccess(rsendRequestStatus, LogSign, clientObj.id, "Successfully sent request to DataNode")
	if rsendRequestStatus == false {
		return
	}

	sendDataStatus := clientObj.SendData(req, arr[0], arr[1])
	logger.LogSuccess(sendDataStatus, LogSign, clientObj.id, "Successfully sent file to DataNode")
	if sendDataStatus == false {
		return
	}

	notification, notificationStatus := clientObj.ReceiveNotification()
	logger.LogSuccess(notificationStatus, LogSign, clientObj.id, fmt.Sprintf("Tracker confirmation: %s", notification))
	if notificationStatus == false {
		return
	}
}

// DownloadHandler A function to handle a download request
func (clientObj *client) DownloadHandler(reqObj request.UploadRequest) {
	serializeRequest := request.SerializeUpload(reqObj)

	sendRequestStatus := clientObj.SendRequest(serializeRequest)
	logger.LogSuccess(sendRequestStatus, LogSign, clientObj.id, "Successfully sent request to tracker")
	if sendRequestStatus == false {
		return
	}

	response, receiveResponseStatus := clientObj.ReceiveResponse()
	logger.LogSuccess(receiveResponseStatus, LogSign, clientObj.id, fmt.Sprintf("Tracker response: %s", response))
	if receiveResponseStatus == false {
		return
	}

	if response == "404: File not found" {
		return
	}

	if response == "All source datanodes are offline, try again later" {
		return
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

		reqObj.ClientPort = reqObj.ClientPort[:3] + strconv.Itoa(i)
		serializeRequest := request.SerializeUpload(reqObj)

		req := serializeRequest + " " + strconv.Itoa(start) + " " + strconv.Itoa(chunkEach)

		start += chunkEach

		rsendRequestStatus := clientObj.RSendRequestToDN(arr[i], arr[i+1]+"1", req)
		logger.LogSuccess(rsendRequestStatus, LogSign, clientObj.id, "Successfully sent request to DataNode")
		if rsendRequestStatus == false {
			return
		}

		go clientObj.RecvPieces(reqObj, strconv.Itoa(blockID), chunkEach, done)

		blockID++
		currPort++
	}

	for i := 0; i < dataNodeCount; i++ {
		logger.LogMsg(LogSign, clientObj.id, fmt.Sprintf("Thread #%d finished %t", i+1, <-done))
	}

	fileutils.AssembleFile(reqObj.FileName, reqObj.FileName[:len(reqObj.FileName)-4], reqObj.FileName[len(reqObj.FileName)-4:], dataNodeCount)
}

// DisplayHandler A function to handle the display request
func (clientObj *client) DisplayHandler(req request.UploadRequest) {
	serializeRequest := request.SerializeUpload(req)

	sendRequestStatus := clientObj.SendRequest(serializeRequest)
	logger.LogSuccess(sendRequestStatus, LogSign, clientObj.id, "Successfully sent request to tracker")
	if sendRequestStatus == false {
		return
	}

	response, receiveResponseStatus := clientObj.ReceiveResponse()
	if receiveResponseStatus == false {
		return
	}

	if response == "No Files" {
		logger.LogMsg(LogSign, clientObj.id, "You have no files")
		return
	}

	clientFiles := strings.Fields(response)

	logger.LogMsg(LogSign, clientObj.id, "Your files:")

	for i := 0; i < len(clientFiles); i += 2 {
		fmt.Printf("%s    %s MB\n", clientFiles[i], clientFiles[i+1])
	}
}
