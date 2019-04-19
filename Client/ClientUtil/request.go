package client

import (
	"fmt"
	"log"
	"strconv"
	"strings"
)

// SerializeRequest A function to serialize a request structure
func SerializeRequest(request Request) string {
	serializedRequest := strconv.Itoa(request.ID) + " " + strconv.Itoa(request.ClientID) + " " +
		request.ClientIP + " " + request.ClientPort + " " + string(request.Type) + " " + request.FileName

	return serializedRequest
}

// DeserializeRequest A function to deserialize a request structure
func DeserializeRequest(serializedRequest string) Request {
	fields := strings.Fields(serializedRequest)
	requestID, _ := strconv.Atoi(fields[0])
	clientID, _ := strconv.Atoi(fields[1])

	requestObj := Request{
		ID:         requestID,
		ClientID:   clientID,
		ClientIP:   fields[2],
		ClientPort: fields[3],
		Type:       RequestType(fields[4]),
		FileName:   fields[5],
	}

	return requestObj
}

// PrintRequest A function to print a request
func PrintRequest(request Request) {
	log.Println("Request info:")
	log.Println("ID = ", request.ID)
	log.Println("ClientID = ", request.ClientID)
	log.Println("ClientPort = ", request.ClientPort)
	log.Println("Type = ", string(request.Type))
	log.Println("FileName = ", request.FileName)
	fmt.Println()
	fmt.Println()
}
