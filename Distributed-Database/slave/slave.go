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
	_ "github.com/lib/pq" // here
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
func registerUser(name string, email string, password string, db *sql.DB) bool {
	sqlStatement := `INSERT INTO clients (name, email, passowrd) VALUES ( $1, $2,$3);`
	fmt.Println("[RegisterUser] Saving user data ..")
	_, err := db.Exec(sqlStatement, name, email, password)
	if err != nil {
		log.Println(err)
	} else {
		fmt.Println("[RegisterUser] Success")
	}

	return true
}
func loginUser(name string, password string, db *sql.DB) bool {
	sqlStatement := `SELECT * FROM clients WHERE email=$1 and passowrd=$2;`

	var clientID int
	var clientName, clientEmail, clientPassword string

	row := db.QueryRow(sqlStatement, name, password)
	switch err := row.Scan(&clientID, &clientName, &clientEmail, &clientPassword); err {
	case sql.ErrNoRows:
		return false
	case nil:
		return true
	default:
		panic(err)

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

//======================= Common Functions ==================

//ReadQueryListner :
func ReadQueryListner(status *string, db *sql.DB, id int) {

	subscriber := initSubscriber("tcp://127.0.0.1:600" + strconv.Itoa(id))
	defer subscriber.Close()

	for {
		s, err := subscriber.Recv(0)
		if err != nil {
			log.Println(err)
			continue
		}
		fmt.Println("[ReadQueryListner] recieved", s)
		email, password, _ := commandDataDeseralizer(s)

		status := loginUser(email, password, db)
		if status {
			fmt.Println("[ReadQueryListner] access granted ")
		} else {
			fmt.Println("[ReadQueryListner] access denied")
		}

	}
}

//TrackerUpdateListner :
func TrackerUpdateListner(status *string, db *sql.DB, id int) {
	subscriber := initSubscriber("tcp://127.0.0.1:500" + strconv.Itoa(id))
	defer subscriber.Close()

	for {
		s, err := subscriber.Recv(0)
		if err != nil {
			log.Println(err)
			continue
		}
		fmt.Println("[TrackerUpdateListner] rec", s)
		name, email, password := commandDataDeseralizer(s)
		registerUser(name, email, password, db)

	}
}

//HeartBeatPublisher :
func HeartBeatPublisher(status *string, id int) {
	publisher := initPublisher("tcp://127.0.0.1:300" + strconv.Itoa(id))

	defer publisher.Close()

	publisher.Bind("tcp://127.0.0.1:300" + strconv.Itoa(id))

	for range time.Tick(time.Second * 2) {
		publisher.Send("Heartbeat", 0)
		log.Println("send", "Heartbeat:"+*status)
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
