package request

import (
	logger "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Log"
	"fmt"
	"strconv"
	"strings"
)

// SerializeDownload A function to serialize an upload request
func SerializeDownload(request DownloadRequest) string {
	serializedRequest := string(request.Type) + " " +
		strconv.Itoa(request.ID) + " " +
		strconv.Itoa(request.ClientID) + " " +
		request.ClientIP + " " +
		request.ClientPort + " " +
		request.FileName

	return serializedRequest
}

// DeserializeDownload A function to deserialize an upload request
func DeserializeDownload(serializedRequest string) DownloadRequest {
	fields := strings.Fields(serializedRequest)
	requestID, err0 := strconv.Atoi(fields[1])
	clientID, err1 := strconv.Atoi(fields[2])

	if (err0 != nil) || (err1 != nil) {
		logger.LogMsg(LogSign, 0, "Upload:Deserialize() Deserialization failed")
		return DownloadRequest{}
	}

	requestObj := DownloadRequest{
		ID:         requestID,
		Type:       Type(fields[0]),
		ClientID:   clientID,
		ClientIP:   fields[3],
		ClientPort: fields[4],
		FileName:   fields[5],
	}

	return requestObj
}

// PrintDownload A function to print an upload request
func PrintDownload(request DownloadRequest) {
	fmt.Println("Request info:")
	fmt.Println("  ID = ", request.ID)
	fmt.Println("  Type = ", string(request.Type))
	fmt.Println("  ClientID = ", request.ClientID)
	fmt.Println("  ClientIP = ", request.ClientIP)
	fmt.Println("  ClientPort = ", request.ClientPort)
	fmt.Println("  FileName = ", request.FileName)
	fmt.Println()
}
