package comm

import "github.com/pebbe/zmq4"

// ACK The synchronous acknowledge message
const ACK string = "ACK"

// Init A function to initialize communication
func Init(socketType zmq4.Type, topic string) (*zmq4.Socket, bool) {
	socket, err := zmq4.NewSocket(socketType)
	status := isOkay(err)

	socket.SetLinger(0)

	if socketType == zmq4.SUB {
		socket.SetSubscribe(topic)
	}

	return socket, status
}

// Connect A function that connects a socket to all provided connections
func Connect(socket *zmq4.Socket, connections []string) {
	for _, connection := range connections {
		socket.Connect(connection)
	}
}

// Bind A function that binds a socket to all provided connections
func Bind(socket *zmq4.Socket, connections []string) {
	for _, connection := range connections {
		socket.Bind(connection)
	}
}

// Disconnect A function that disconnects a socket from all provided connections
func Disconnect(socket *zmq4.Socket, connections []string) {
	for _, connection := range connections {
		socket.Disconnect(connection)
	}
}

// SendString A function that synchronously sends a string on a socket
func SendString(socket *zmq4.Socket, msg string) bool {
	_, sendErr := socket.Send(msg, 0)
	acknowledge, recvErr := socket.Recv(0)

	status := isOkay(sendErr) && isOkay(recvErr) && (acknowledge == ACK)

	return status
}

// SendBytes A function that synchronously sends an array of bytes on a socket
func SendBytes(socket *zmq4.Socket, data []byte) bool {
	_, sendErr := socket.SendBytes(data, 0)
	acknowledge, recvErr := socket.Recv(0)

	status := isOkay(sendErr) && isOkay(recvErr) && (acknowledge == ACK)

	return status
}

// RecvString A function that synchronously receives a string from a socket
func RecvString(socket *zmq4.Socket) (string, bool) {
	msg, recvErr := socket.Recv(0)
	status := isOkay(recvErr) && (msg != "")

	if status == true {
		socket.Send(ACK, 0)
	}

	return msg, status
}

// RecvBytes A function that synchronously sends an array of bytes from a socket
func RecvBytes(socket *zmq4.Socket) ([]byte, bool) {
	data, recvErr := socket.RecvBytes(0)
	status := isOkay(recvErr)

	if status == true {
		socket.Send(ACK, 0)
	}

	return data, status
}

// GetConnectionString A function to formulate the connection string given IP and Port
func GetConnectionString(ip string, port string) string {
	connectionString := "tcp://" + ip + ":" + port

	return connectionString
}

// isOkay A function to check if there is an error
func isOkay(err error) bool {
	if err == nil {
		return true
	}
	return false
}
