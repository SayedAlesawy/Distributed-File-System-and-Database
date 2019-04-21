package main

import (
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/pebbe/zmq4"
)

//======================= Common Functions ==================
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

func getOnlineSlaves(BeatStamps []time.Time) []int {
	onlineSlaves := make([]int, 0)

	for i := range BeatStamps {
		delay := time.Now().Sub(BeatStamps[i]) / 1000000000
		if delay > 4 {
			fmt.Println("[OnlineSlavesFetcher] Slave[" + strconv.Itoa(i) + "] Disqualified for been away(+4 seconds)")
		} else {
			onlineSlaves = append(onlineSlaves, i)
		}

	}
	return onlineSlaves
}

//======================= Common Functions ==================

//ListenToClientReq :
func ListenToClientReq(InsertionsStack [][]string, BeatStamp []time.Time, slaveIPs []string, clientIP string) {

	clientSubscriber := initSubscriber(clientIP + "9092")

	defer clientSubscriber.Close()

	clientPublisher := initPublisher(clientIP + "8092")

	defer clientPublisher.Close()

	for {
		s, err := clientSubscriber.Recv(0)
		if err != nil {
			log.Println(err)
			continue
		}

		commandType, commandData := commandDeseralizer(s)
		fmt.Println("[ClientSubscriber] rec", commandType)

		if commandType == "" {
			fmt.Println("[ClientSubscriber] Dropping Message as invalid :" + s)
			continue
		}

		onlineSlaves := getOnlineSlaves(BeatStamp)
		rand.Seed(time.Now().Unix())
		chosenSlave := -1

		if len(onlineSlaves) > 0 {
			chosenSlave = onlineSlaves[rand.Intn(len(onlineSlaves))]
		}

		if strings.Compare(commandType, "REGISTER") == 0 {
			fmt.Println("[ClientSubscriber] Sending Command Data to DB Execution Layer")
			registerUser(commandDataDeseralizer(commandData))
			fmt.Println("[ClientSubscriber] Adding InsertionQuery to all slaves :", commandData)

			for i := range InsertionsStack {
				InsertionsStack[i] = append(InsertionsStack[i], commandData)
			}

		} else if strings.Compare(commandType, "LOGIN") == 0 {
			if chosenSlave != -1 {
				fmt.Println("[ClientSubscriber] Assigning ReadQuery to slave ["+strconv.Itoa(chosenSlave)+"]  :", s)
				clientPublisher.Send(slaveIPs[chosenSlave]+":600"+strconv.Itoa(1+chosenSlave), 0)
			}

		}

	}
}

//ListenToHeartBeat :
func ListenToHeartBeat(InsertionsStack [][]string, id int, BeatStamp []time.Time, slaveIP string) {

	slaveSubscriber := initSubscriber(slaveIP + "300" + strconv.Itoa(id+1))
	defer slaveSubscriber.Close()
	slavePublisher := initPublisher(slaveIP + "500" + strconv.Itoa(id+1))

	defer slavePublisher.Close()

	for {
		s, err := slaveSubscriber.Recv(0)
		if err != nil {
			log.Println(err)
			continue
		}
		fmt.Println("[SlaveSubscriber] rec", s)
		BeatStamp[id] = time.Now()
		stackSize := len(InsertionsStack[id])

		if stackSize > 0 {
			fmt.Println("[HeartBeatSubscriber] Updating slave[" + strconv.Itoa(id) + "] :  ")

			for i := range InsertionsStack[id] {
				fmt.Println("[HeartBeatSubscriber] Sending Query[" + strconv.Itoa(id) + "] : " + InsertionsStack[id][i])
				fmt.Println("[DEBUG]" + slaveIP + "500" + strconv.Itoa(id+1))
				slavePublisher.Send(InsertionsStack[id][i], 0)
			}
			InsertionsStack[id] = InsertionsStack[id][:0]
		} else {
			fmt.Println("[HeartBeatSubscriber] Slave[" + strconv.Itoa(id) + "] is up to date")
		}

	}
}

func main() {
	InsertionsStack := make([][]string, 3)
	BeatStamp := make([]time.Time, 3)
	slaves := make([]string, 3)
	clientIP := "tcp://127.0.0.1:"
	fmt.Println()
	for i := range slaves {
		slaves[i] = "tcp://127.0.0.1:"
	}

	for i := range InsertionsStack {
		InsertionsStack[i] = make([]string, 0)
	}
	go ListenToClientReq(InsertionsStack, BeatStamp, slaves, clientIP)
	for i := range slaves {
		go ListenToHeartBeat(InsertionsStack, i, BeatStamp, slaves[i])
	}

	for {

	}
}
