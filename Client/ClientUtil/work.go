package client

import (
	fileutils "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/File"
	logger "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Log"
	request "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Request"
	"bufio"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"fmt"

	"github.com/pebbe/zmq4"
)

// UploadHandler A function to handle a upload request
func (clientObj *client) UploadHandler(req request.UploadRequest) {
	exists := fileutils.IsThere(req.FileName)
	if exists == false {
		logger.LogMsg(LogSign, clientObj.id, fmt.Sprintf("%s :no such file or directory", req.FileName))
		return
	}

	serializeRequest := request.SerializeUpload(req)

	sendRequestStatus := clientObj.SendRequest(serializeRequest)
	logger.LogSuccess(sendRequestStatus, LogSign, clientObj.id, "Successfully sent request to tracker")
	if sendRequestStatus == false {
		return
	}

	response, receiveResponseStatus := clientObj.ReceiveResponse()
	logger.LogSuccess(receiveResponseStatus, LogSign, clientObj.id, fmt.Sprintf("Tracker response: %s", response))
	if receiveResponseStatus == false {
		return
	}

	if response == "All data nodes are offline" {
		return
	}

	arr := strings.Fields(response)

	rsendRequestStatus := clientObj.RSendRequestToDN(arr[0], arr[1], serializeRequest)
	logger.LogSuccess(rsendRequestStatus, LogSign, clientObj.id, "Successfully sent request to DataNode")
	if rsendRequestStatus == false {
		return
	}

	sendDataStatus := clientObj.SendData(req, arr[0], arr[1])
	logger.LogSuccess(sendDataStatus, LogSign, clientObj.id, "Successfully sent file to DataNode")
	if sendDataStatus == false {
		return
	}

	notification, notificationStatus := clientObj.ReceiveNotification()
	logger.LogSuccess(notificationStatus, LogSign, clientObj.id, fmt.Sprintf("Tracker confirmation: %s", notification))
	if notificationStatus == false {
		return
	}
}

// DownloadHandler A function to handle a download request
func (clientObj *client) DownloadHandler(reqObj request.UploadRequest) {
	serializeRequest := request.SerializeUpload(reqObj)

	sendRequestStatus := clientObj.SendRequest(serializeRequest)
	logger.LogSuccess(sendRequestStatus, LogSign, clientObj.id, "Successfully sent request to tracker")
	if sendRequestStatus == false {
		return
	}

	response, receiveResponseStatus := clientObj.ReceiveResponse()
	logger.LogSuccess(receiveResponseStatus, LogSign, clientObj.id, fmt.Sprintf("Tracker response: %s", response))
	if receiveResponseStatus == false {
		return
	}

	if response == "404: File not found" {
		return
	}

	if response == "All source datanodes are offline, try again later" {
		return
	}

	arr := strings.Fields(response)
	chunkCount, _ := strconv.Atoi(arr[0])
	dataNodeCount := (len(arr) - 1) / 2
	chunkEach := chunkCount / dataNodeCount
	start := 0
	blockID := 1
	currPort := 1

	done := make(chan bool)

	for i := 1; i < len(arr)-1; i += 2 {
		if i == len(arr)-2 {
			chunkEach += (chunkCount % dataNodeCount)
		}

		reqObj.ClientPort = reqObj.ClientPort[:3] + strconv.Itoa(i)
		serializeRequest := request.SerializeUpload(reqObj)

		req := serializeRequest + " " + strconv.Itoa(start) + " " + strconv.Itoa(chunkEach)

		start += chunkEach

		rsendRequestStatus := clientObj.RSendRequestToDN(arr[i], arr[i+1]+"1", req)
		logger.LogSuccess(rsendRequestStatus, LogSign, clientObj.id, "Successfully sent request to DataNode")
		if rsendRequestStatus == false {
			return
		}

		go clientObj.RecvPieces(reqObj, strconv.Itoa(blockID), chunkEach, done)

		blockID++
		currPort++
	}

	for i := 0; i < dataNodeCount; i++ {
		logger.LogMsg(LogSign, clientObj.id, fmt.Sprintf("Thread #%d finished %t", i+1, <-done))
	}

	fileutils.AssembleFile(reqObj.FileName, reqObj.FileName[:len(reqObj.FileName)-4], reqObj.FileName[len(reqObj.FileName)-4:], dataNodeCount)
}

// DisplayHandler A function to handle the display request
func (clientObj *client) DisplayHandler(req request.UploadRequest) {
	serializeRequest := request.SerializeUpload(req)

	sendRequestStatus := clientObj.SendRequest(serializeRequest)
	logger.LogSuccess(sendRequestStatus, LogSign, clientObj.id, "Successfully sent request to tracker")
	if sendRequestStatus == false {
		return
	}

	response, receiveResponseStatus := clientObj.ReceiveResponse()
	if receiveResponseStatus == false {
		return
	}

	if response == "No Files" {
		logger.LogMsg(LogSign, clientObj.id, "You have no files")
		return
	}

	clientFiles := strings.Fields(response)

	logger.LogMsg(LogSign, clientObj.id, "Your files:")

	for i := 0; i < len(clientFiles); i += 2 {
		fmt.Printf("%s    %s MB\n", clientFiles[i], clientFiles[i+1])
	}
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

//GetClientID :
func GetClientID() string {

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
