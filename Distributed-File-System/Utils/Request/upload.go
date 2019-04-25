package request

import (
	logger "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Log"
	"fmt"
	"strconv"
	"strings"
)

// SerializeUpload A function to serialize an upload request
func SerializeUpload(request UploadRequest) string {
	serializedRequest := string(request.Type) + " " +
		strconv.Itoa(request.ID) + " " +
		strconv.Itoa(request.ClientID) + " " +
		request.ClientIP + " " +
		request.ClientPort + " " +
		request.FileName

	return serializedRequest
}

// DeserializeUpload A function to deserialize an upload request
func DeserializeUpload(serializedRequest string) UploadRequest {
	fields := strings.Fields(serializedRequest)
	requestID, err0 := strconv.Atoi(fields[1])
	clientID, err1 := strconv.Atoi(fields[2])

	if (err0 != nil) || (err1 != nil) {
		logger.LogMsg(LogSign, 0, "Upload:Deserialize() Deserialization failed")
		return UploadRequest{}
	}

	requestObj := UploadRequest{
		ID:         requestID,
		Type:       Type(fields[0]),
		ClientID:   clientID,
		ClientIP:   fields[3],
		ClientPort: fields[4],
		FileName:   fields[5],
	}

	return requestObj
}

// PrintUpload A function to print an upload request
func PrintUpload(request UploadRequest) {
	fmt.Println("Request info:")
	fmt.Println("  ID = ", request.ID)
	fmt.Println("  Type = ", string(request.Type))
	fmt.Println("  ClientID = ", request.ClientID)
	fmt.Println("  ClientIP = ", request.ClientIP)
	fmt.Println("  ClientPort = ", request.ClientPort)
	fmt.Println("  FileName = ", request.FileName)
	fmt.Println()
}
