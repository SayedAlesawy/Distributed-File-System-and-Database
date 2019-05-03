package request

import "strings"

// LogSign Used for logging Request related messages
const LogSign string = "[Request]"

// Type An Enum to represent the different types of client requests
type Type string

const (
	//Download A Download request (dwn)
	Download Type = "dwn"

	//Upload A Upload request (up)
	Upload Type = "up"

	//Display A Download request (ls)
	Display Type = "ls"

	//Replicate A replication request
	Replicate Type = "rep"

	//Completion An action completion notification
	Completion Type = "comp"

	//Invalid An error type
	Invalid Type = "inv"
)

// UploadRequest Represents an upload request
type UploadRequest struct {
	ID         int    //The ID of the request
	Type       Type   //Represents the type of a request
	ClientID   int    //The ID of the client who sent the requst
	ClientIP   string //The IP of the client who sent the request
	ClientPort string //The port of the client who sent the request
	FileName   string //The name of the file to be uploaded
}

// DownloadRequest Represents a download request
type DownloadRequest struct {
	ID         int    //The ID of the request
	Type       Type   //Represents the type of a request
	ClientID   int    //The ID of the client who sent the requst
	ClientIP   string //The IP of the client who sent the request
	ClientPort string //The port of the client who sent the request
	FileName   string //The name of the file to be uploaded
}

// ReplicationRequest Represents a replication request
type ReplicationRequest struct {
	ID                 int    //The ID of the replication request
	Type               Type   //Represents the type of a request
	ClientID           int    //The client ID associated with the replicated file
	FileName           string //The file name to be replicated
	SourceID           int    //The ID of the source Data Node
	TargetNodeID       int    //The ID of the target machine
	TargetNodeIP       string //The IP of the target machine (connect there)
	TargetNodeBasePort string //The replication port of the target machine (connect there)
}

// CompletionRequest Represents a notification of an action completion
type CompletionRequest struct {
	Type       Type   //The request type
	ClientID   int    //The ID for the client
	ClientIP   string //The IP of the client
	ClientPort string //The port of the client
	FileName   string //The name of the file
	FileSize   int    //The size of the file
	Location   string //A concatenated string of all locations where the file exists
}

// GetType A function to get the type of a request
func GetType(req string) Type {
	reqType := strings.Fields(req)[0]

	if reqType == "dwn" {
		return Download

	} else if reqType == "up" {
		return Upload

	} else if reqType == "ls" {
		return Display

	} else if reqType == "rep" {
		return Replicate

	} else if reqType == "comp" {
		return Completion

	} else {
		return Invalid
	}
}
