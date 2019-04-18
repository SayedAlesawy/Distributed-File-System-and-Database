package client

import (
	"strconv"
	"strings"
)

// serializeRequest A function to serialize a request structure
func serializeRequest(request Request) string {
	serializedRequest := strconv.Itoa(request.ID) + " " + strconv.Itoa(request.ClientID) + " " +
		request.ClientIP + " " + request.ClientPort + " " + string(request.Type) + " " + request.FileName

	return serializedRequest
}

func deserializeRequest(serializedRequest string) Request {
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
