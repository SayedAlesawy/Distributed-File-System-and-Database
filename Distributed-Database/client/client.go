package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
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

func initSubscriber(addr string) *zmq4.Socket {
	subscriber, _ := zmq4.NewSocket(zmq4.SUB)
	subscriber.SetLinger(0)

	subscriber.Connect(addr)
	subscriber.SetSubscribe("")
	return subscriber
}

//AssignedSlaveListner :
func AssignedSlaveListner(command *string, clientID *string, trackerIP string) {
	subscriber, _ := zmq4.NewSocket(zmq4.SUB)
	subscriber.SetLinger(0)
	defer subscriber.Close()
	slavelist := make([]*zmq4.Socket, 3)
	idSubs := make([]*zmq4.Socket, 3)
	//for i := range slavelist {
	//	slavelist[i] = initPublisher("tcp://127.0.0.1:600" + strconv.Itoa(i+1))
	//}
	slavelist[0] = initPublisher("tcp://127.0.0.1:600" + strconv.Itoa(0+1))
	slavelist[1] = initPublisher("tcp://127.0.0.1:600" + strconv.Itoa(1+1))
	slavelist[2] = initPublisher("tcp://127.0.0.1:600" + strconv.Itoa(2+1))

	idSubs[0] = initSubscriber("tcp://127.0.0.1:8093")
	idSubs[1] = initSubscriber("tcp://127.0.0.1:8093")
	idSubs[2] = initSubscriber("tcp://127.0.0.1:8093")

	subscriber.Connect(trackerIP + "8092")
	subscriber.SetSubscribe("")

	//idSub := initSubscriber("tcp://127.0.0.1:8093")

	for {
		if strings.Split(*command, ":")[0] != "LOGIN" {
			continue
		}
		s, err := subscriber.Recv(0)
		if err != nil {
			log.Println(err)
			continue
		}
		fmt.Println("[AssignedSlaveListner] Recieved Slave info : " + s)

		fmt.Println("[AssignedSlaveListner] Sending Query to Assigned Slave : " + *command)

		sID, _ := strconv.ParseInt(s, 10, 64)

		slavelist[sID-1].Send(*command, 0)

		*clientID, err = idSubs[sID-1].Recv(0)
		if err == nil {
			fmt.Println("[AssignedSlaveListner] Recieved ID = " + *clientID)
		}
		//*command = ""

	}
}

func getClientID() string {

	trackerIP := "tcp://127.0.0.1:"
	command := ""
	clientID := ""
	publisher := initPublisher(trackerIP + "9092")

	defer publisher.Close()

	go AssignedSlaveListner(&command, &clientID, trackerIP)
	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Println("Your ID : " + clientID)
		if clientID != "" && clientID != "-15" {
			return clientID
		}

		fmt.Print("LOGIN/REGISTER?(L/R)")
		command, _ = reader.ReadString('\n')
		if strings.Compare(command, "R\n") == 0 {
			fmt.Println("ENTER REGISTER USER INFORMATION")
			fmt.Print("name :")
			name, _ := reader.ReadString('\n')
			fmt.Print("email :")
			email, _ := reader.ReadString('\n')
			fmt.Print("password :")
			password, _ := reader.ReadString('\n')

			name = strings.Replace(name, "\n", "", -1)
			email = strings.Replace(email, "\n", "", -1)
			password = strings.Replace(password, "\n", "", -1)
			command = "REGISTER:" + name + ";" + email + ";" + password
			publisher.Send("REGISTER:"+name+";"+email+";"+password, 0)
			fmt.Println("[MainThread]", "REGISTER:"+name+";"+email+";"+password)

		} else {
			fmt.Println("ENTER LOGIN USER INFORMATION")
			fmt.Print("email :")
			email, _ := reader.ReadString('\n')
			fmt.Print("password :")
			password, _ := reader.ReadString('\n')

			email = strings.Replace(email, "\n", "", -1)
			password = strings.Replace(password, "\n", "", -1)
			command = "LOGIN:" + email + ";" + password
			publisher.Send("LOGIN:"+email+";"+password, 0)
			fmt.Println("[MainThread]", "LOGIN:"+email+";"+password)
			time.Sleep(1 * time.Second)

		}

	}

}

func main() {
	getClientID()
}
