package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq" // here
	"github.com/pebbe/zmq4"
)

//ReadQueryListner :
func ReadQueryListner(status *string, db *sql.DB, id int) {

	subscriber, _ := zmq4.NewSocket(zmq4.SUB)
	subscriber.SetLinger(0)
	defer subscriber.Close()

	subscriber.Connect("tcp://127.0.0.1:600" + strconv.Itoa(id))
	subscriber.SetSubscribe("")
	for {
		s, err := subscriber.Recv(0)
		if err != nil {
			log.Println(err)
			continue
		}
		fmt.Println("[ReadQueryListner] rec", s)

	}
}

//TrackerUpdateListner :
func TrackerUpdateListner(status *string, db *sql.DB, id int) {
	subscriber, _ := zmq4.NewSocket(zmq4.SUB)
	subscriber.SetLinger(0)
	defer subscriber.Close()

	subscriber.Connect("tcp://127.0.0.1:500" + strconv.Itoa(id))
	subscriber.SetSubscribe("")
	for {
		s, err := subscriber.Recv(0)
		if err != nil {
			log.Println(err)
			continue
		}
		fmt.Println("[TrackerUpdateListner] rec", s)
		ExecuteQuery(s, db)

	}
}

//HeartBeatPublisher :
func HeartBeatPublisher(status *string, id int) {
	publisher, err := zmq4.NewSocket(zmq4.PUB)
	if err != nil {
		fmt.Print(err)
		return
	}

	publisher.SetLinger(0)
	defer publisher.Close()

	publisher.Bind("tcp://127.0.0.1:300" + strconv.Itoa(id))

	for range time.Tick(time.Second * 2) {
		publisher.Send("Heartbeat", 0)
		log.Println("send", "Heartbeat:"+*status)
	}
}

func connectDB() *sql.DB {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("[DB]Error loading .env file")
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
	db := connectDB()
	defer db.Close()
	status := "Avaliable"
	id := 2
	go HeartBeatPublisher(&status, id+1)
	go TrackerUpdateListner(&status, db, id+1)
	go ReadQueryListner(&status, db, id+1)
	for {

	}
}
