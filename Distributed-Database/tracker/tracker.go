package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/pebbe/zmq4"

	_ "github.com/lib/pq" // here
)

//ListenToClientReq :
func ListenToClientReq(InsertionsStack [][]string, BeatStamp []time.Time) {

	db := connectDB()
	defer db.Close()

	subscriber, _ := zmq4.NewSocket(zmq4.SUB)
	subscriber.SetLinger(0)
	defer subscriber.Close()

	subscriber.Connect("tcp://127.0.0.1:9092")
	subscriber.SetSubscribe("")

	publisher, err := zmq4.NewSocket(zmq4.PUB)
	if err != nil {
		fmt.Print(err)
		return
	}

	publisher.SetLinger(0)
	defer publisher.Close()

	publisher.Bind("tcp://127.0.0.1:8092")

	for {
		s, err := subscriber.Recv(0)
		if err != nil {
			log.Println(err)
			continue
		}
		fmt.Println("[ClientSubscriber] rec", s[0:6])

		if strings.Compare(s[0:6], "INSERT") == 0 {
			ExecuteQuery(s[7:len(s)], db)

			for i := range BeatStamp {
				delay := time.Now().Sub(BeatStamp[i]) / 1000000000
				if delay > 4 {
					fmt.Println("[ClientSubscriber] Slave[" + strconv.Itoa(i) + "] Disqualified for been away")
				} else {
					InsertionsStack[i] = append(InsertionsStack[i], s[7:len(s)])
					fmt.Println("[ClientSubscriber] Adding InsertionQuery to slave ["+strconv.Itoa(i)+"]  :", s[7:len(s)])
				}

			}
		} else {

			for i := range BeatStamp {
				delay := time.Now().Sub(BeatStamp[i]) / 1000000000
				if delay > 4 {
					fmt.Println("[ClientSubscriber] Slave[" + strconv.Itoa(i) + "] Disqualified for been away")
				} else {
					fmt.Println("[ClientSubscriber] Assigning ReadQuery to slave ["+strconv.Itoa(i)+"]  :", s)
					publisher.Send("tcp://127.0.0.1:600"+strconv.Itoa(1+i), 0)
					break
				}

			}
		}

	}
}

//ListenToHeartBeat :
func ListenToHeartBeat(InsertionsStack [][]string, id int, BeatStamp []time.Time) {

	subscriber, _ := zmq4.NewSocket(zmq4.SUB)
	subscriber.SetLinger(0)
	defer subscriber.Close()

	publisher, _ := zmq4.NewSocket(zmq4.PUB)
	publisher.SetLinger(0)
	defer publisher.Close()

	subscriber.Connect("tcp://127.0.0.1:300" + strconv.Itoa(id+1))
	subscriber.SetSubscribe("")

	publisher.Bind("tcp://127.0.0.1:500" + strconv.Itoa(id+1))
	for {
		s, err := subscriber.Recv(0)
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
				publisher.Send(InsertionsStack[id][i], 0)
			}
			InsertionsStack[id] = InsertionsStack[id][:0]
		} else {
			fmt.Println("[HeartBeatSubscriber] Slave[" + strconv.Itoa(id) + "] is up to date")
		}

	}
}

func connectDB() *sql.DB {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("[DB] Error loading .env file")
	}

	host := os.Getenv("HOST")
	port := os.Getenv("PORT")
	user := os.Getenv("USER_NAME")
	password := os.Getenv("PASSWORD")
	dbname := os.Getenv("DB_NAME")

	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	fmt.Println("[DB] Successfully connected!")
	return db
}

// ExecuteQuery ;
func ExecuteQuery(sqlStatement string, db *sql.DB) {

	_, err := db.Exec(sqlStatement)
	if err != nil {
		log.Println(err)
	} else {
		fmt.Println("[DB] Successfully Executed!")
	}

}
func main() {
	InsertionsStack := make([][]string, 3)
	BeatStamp := make([]time.Time, 3)

	for i := range InsertionsStack {
		InsertionsStack[i] = make([]string, 0)
	}
	go ListenToClientReq(InsertionsStack, BeatStamp)
	go ListenToHeartBeat(InsertionsStack, 0, BeatStamp)
	go ListenToHeartBeat(InsertionsStack, 1, BeatStamp)
	go ListenToHeartBeat(InsertionsStack, 2, BeatStamp)

	for {

	}
}
