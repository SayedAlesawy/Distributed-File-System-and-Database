package common

import (
	"fmt"
	"strings"

	"github.com/pebbe/zmq4"
)

func initSubscriber(addr string) *zmq4.Socket {
	subscriber, _ := zmq4.NewSocket(zmq4.SUB)
	subscriber.SetLinger(0)

	subscriber.Connect(addr)
	subscriber.SetSubscribe("")
	return subscriber
}

func initPublisher(addr string) *zmq4.Socket {
	publisher, err := zmq4.NewSocket(zmq4.PUB)
	if err != nil {
		fmt.Print(err)
		return nil
	}
	publisher.SetLinger(0)
	publisher.Bind(addr)
	return publisher
}
func commandDeseralizer(s string) (string, string) {
	fields := strings.Split(s, ":")
	if len(fields) < 2 {
		return "", ""
	}
	return fields[0], fields[1]
}
func commandDataDeseralizer(s string) (string, string, string) {
	fields := strings.Split(s, ";")
	if len(fields) < 3 {
		if len(fields) < 2 {
			return fields[0], "", ""
		}
		return fields[0], fields[1], ""
	}
	return fields[0], fields[1], fields[2]
}
func registerUser(name string, email string, password string) bool {
	fmt.Println("[RegisterUser] Saving user data ..")
	fmt.Println("[RegisterUser] Success")
	return true
}
