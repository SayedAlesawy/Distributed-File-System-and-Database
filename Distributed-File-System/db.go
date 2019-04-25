package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq" // here
)

// Migrate A function to perform DB migration
func Migrate(db *sql.DB) {

	sqlStatement :=
		`	
					DROP TABLE datanodes;
					CREATE TABLE datanodes (
					id SERIAL PRIMARY KEY,
					DataNodeID int UNIQUE NOT NULL,
					IP varchar(60) NOT NULL
					);`

	sqlStatement += `
					DROP TABLE metafiles;
					CREATE TABLE metafiles (
					id SERIAL PRIMARY KEY,
					FileName varchar(60)  NOT NULL,
					ClientID int NOT NULL
					);
					ALTER TABLE metafiles
					ADD CONSTRAINT unq_filename_clientid
					Upeer_idNIQUE (FileName, ClientID) ;
				`

	_, err := db.Exec(sqlStatement)
	if err != nil {
		fmt.Println(err)
	}
}
func deleteDatanode(DataNodeID int, db *sql.DB) {
	_, err := db.Exec("DELETE FROM datanodes WHERE DataNodeID=$1", DataNodeID)
	if err != nil {
		log.Println(err)
	} else {
		fmt.Println("[DB] Success")
	}
}
func deleteMetafile(filename string, ClientID string, db *sql.DB) {
	_, err := db.Exec("DELETE FROM metafiles WHERE ClientID=$1 and FileName=$2", ClientID, filename)
	if err != nil {
		log.Println(err)
	} else {
		fmt.Println("[DB] Success")
	}
}

func getMetafilesforclient(clientID string, db *sql.DB) {

	rows, err := db.Query("SELECT FileName FROM metafiles where  ClientID = $1", clientID)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	var filenames []string
	for rows.Next() {
		var filename string

		err = rows.Scan(&filename)
		if err != nil {
			panic(err)
		}
		filenames = append(filenames, filename)

	}
	// get any error encountered during iteration
	err = rows.Err()
	if err != nil {
		panic(err)
	}
}

func createDataNode(db *sql.DB, dnid int, ip string) bool {

	sqlStatement :=
		`
		INSERT INTO datanodes (DataNodeID,IP)
		VALUES ($1,$2)
		`

	_, err := db.Exec(sqlStatement, dnid, ip)

	if err != nil {
		fmt.Println(err)
		return false
	} else {
		fmt.Println("[DB] Created datanode successfully")
		return true
	}

}
func createmetafile(db *sql.DB, clientID int, filename string) bool {
	sqlStatement :=
		`
		INSERT INTO metafiles (ClientID,FileName)
		VALUES ($1,$2)
		`

	_, err := db.Exec(sqlStatement, clientID, filename)

	if err != nil {
		fmt.Println(err)
		return false
	} else {
		fmt.Println("[DB] Created metafile successfully")
		return true
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
	/*
		//for mysql + change the _ import
		db, err := sql.Open("mysql", "root:.1@/os_db")
	*/
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
func main() {
	Migrate(connectDB())
}
