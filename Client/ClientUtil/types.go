package client

import "github.com/pebbe/zmq4"

// client A struct to represent the client structure
type client struct {
	id           int
	ip           string
	port         string
	trackerIP    string
	trackerPorts []string
	socket       *zmq4.Socket
}

// RequestType An Enum to represent the different types of client requests
type RequestType string

const (
	//Download A Download request (dwn)
	Download RequestType = "dwn"

	//Upload A Upload request (up)
	Upload RequestType = "up"

	//Display A Download request (ls)
	Display RequestType = "ls"
)

// Request A struct to represent the client request
type Request struct {
	ID         int
	ClientID   int
	ClientIP   string
	ClientPort string
	FileName   string
	Type       RequestType
}

// NewClient A constructor function for the client type
func NewClient(_id int, _ip string, _port string, _trackerIP string, _trackerPorts []string) client {
	clientObj := client{
		id:           _id,
		ip:           _ip,
		port:         _port,
		trackerIP:    _trackerIP,
		trackerPorts: _trackerPorts,
	}

	return clientObj
}
