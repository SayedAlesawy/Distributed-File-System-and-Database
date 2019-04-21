package main

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/pebbe/zmq4"
)

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

//AssignedSlaveListner :
func AssignedSlaveListner() {
	subscriber, _ := zmq4.NewSocket(zmq4.SUB)
	subscriber.SetLinger(0)
	defer subscriber.Close()
	slavelist := make([]*zmq4.Socket, 3)
	for i := range slavelist {
		slavelist[i] = initPublisher("tcp://127.0.0.1:600" + strconv.Itoa(i+1))
	}

	subscriber.Connect("tcp://127.0.0.1:8092")
	subscriber.SetSubscribe("")
	for {
		s, err := subscriber.Recv(0)
		if err != nil {
			log.Println(err)
			continue
		}
		fmt.Println("[AssignedSlaveListner] Recieved Slave info : " + s)

		sID, _ := strconv.ParseInt(s, 10, 64)

		slavelist[sID-1].Send("READ   ", 0)
		fmt.Println("[AssignedSlaveListner] Sending Query to Assigned Slave")

	}
}

func main() {
	publisher := initPublisher("tcp://127.0.0.1:9092")

	defer publisher.Close()

	go AssignedSlaveListner()

	for range time.Tick(time.Second) {
		publisher.Send("LOGIN:kareem;k@mail.com;12345678", 0)
		fmt.Println("[MainThread]", "REGISTER:kareem;k@mail.com;12345678")
	}
}
