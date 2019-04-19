# Distributed-Video-Processing-Cluster
A distributed cluster for video processing tasks.

## Distributed Database Configuration
Instructions for configuring MySQL server:

- First ,install mysql:
    ```
    sudo apt-get install mysql-server
    ```
- Start the server and add  it to startup apps
    ```
    systemctl start mysql
    sudo systemctl enable mysql
    ```
- Login as root with your sudo password
    ```
    /usr/bin/mysql -u root -p
    ```
- Create a database and give it a name
    ```
    CREATE DATABASE os_db;
    ```
- Install the corresponding go package : https://github.com/go-sql-driver/mysql#usage

- Test your configuration from the package wiki examples
    ```
        package main
        import "database/sql"
        import _ "github.com/go-sql-driver/mysql"


        func main(){
            db, err := sql.Open("mysql", "root:password@/os_db")
            if err != nil {
                panic(err.Error()) 
            }
            defer db.Close()

            // Open doesn't open a connection. Validate DSN data:
            err = db.Ping()
            if err != nil {
                panic(err.Error()) 
            }
            
        }

    ```