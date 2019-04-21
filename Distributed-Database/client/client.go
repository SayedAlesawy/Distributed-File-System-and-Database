package main

import (
	"fmt"
	"log"
	"time"

	"github.com/pebbe/zmq4"
)

//AssignedSlaveListner :
func AssignedSlaveListner() {
	subscriber, _ := zmq4.NewSocket(zmq4.SUB)
	subscriber.SetLinger(0)
	defer subscriber.Close()

	subscriber.Connect("tcp://127.0.0.1:8092")
	subscriber.SetSubscribe("")
	for {
		s, err := subscriber.Recv(0)
		if err != nil {
			log.Println(err)
			continue
		}
		fmt.Println("[AssignedSlaveListner] Recieved Slave info : " + s)
		publisher, err := zmq4.NewSocket(zmq4.PUB)
		if err != nil {
			fmt.Print(err)
			return
		}

		publisher.SetLinger(0)
		publisher.Bind(s)

		publisher.Send("READ   ", 0)
		fmt.Println("[AssignedSlaveListner] Sending Query to Assigned Slave")

		defer publisher.Close()
	}
}

func main() {
	publisher, err := zmq4.NewSocket(zmq4.PUB)
	if err != nil {
		fmt.Print(err)
		return
	}

	publisher.SetLinger(0)
	defer publisher.Close()

	publisher.Bind("tcp://127.0.0.1:9092")

	go AssignedSlaveListner()

	for range time.Tick(time.Second) {
		publisher.Send("REGISTER:kareem;k@mail.com;12345678", 0)
		fmt.Println("[MainThread]", "REGISTER:kareem;k@mail.com;12345678")
	}
}
