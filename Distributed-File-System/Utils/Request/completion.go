package request

import (
	logger "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Log"
	"fmt"
	"strconv"
	"strings"
)

// SerializeCompletion A function to serialize a completion request
func SerializeCompletion(request CompletionRequest) string {
	serializedRequest := string(request.Type) + " " +
		strconv.Itoa(request.ClientID) + " " +
		request.ClientIP + " " +
		request.ClientPort + " " +
		request.FileName + " " +
		strconv.Itoa(request.FileSize) + " " +
		request.Location

	return serializedRequest
}

// DeserializeCompletion A function to deserialize a completion request
func DeserializeCompletion(serializedRequest string) CompletionRequest {
	fields := strings.Fields(serializedRequest)
	clientID, err0 := strconv.Atoi(fields[1])
	fileSize, err1 := strconv.Atoi(fields[5])

	if (err0 != nil) || (err1 != nil) {
		logger.LogMsg(LogSign, 0, "Completion:Deserialize() Deserialization failed")
		return CompletionRequest{}
	}

	location := ""
	for i := 6; i < len(fields); i++ {
		location += fields[i]
		if i < len(fields)-1 {
			location += " "
		}
	}

	requestObj := CompletionRequest{
		Type:       Type(fields[0]),
		ClientID:   clientID,
		ClientIP:   fields[2],
		ClientPort: fields[3],
		FileName:   fields[4],
		FileSize:   fileSize,
		Location:   location,
	}

	return requestObj
}

// PrintCompletion A function to print a completion request
func PrintCompletion(request CompletionRequest) {
	fmt.Println("Request info:")
	fmt.Println("  Type = ", string(request.Type))
	fmt.Println("  ClientID = ", request.ClientID)
	fmt.Println("  ClientIP = ", request.ClientIP)
	fmt.Println("  ClientPort = ", request.ClientPort)
	fmt.Println("  FileName = ", request.FileName)
	fmt.Println("  FileSize = ", request.FileSize)
	fmt.Println("  Location = ", request.Location)
	fmt.Println()
}
