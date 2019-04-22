package logger

import "log"

// LogMsg A function to log messages
func LogMsg(sign string, id int, msg string) {
	log.Println(sign, "#", id, msg)
}

// LogFail A function to log failure messages
func LogFail(ok bool, sign string, id int, msg string) {
	if ok == false {
		LogMsg(sign, id, msg)
	}
}

// LogSuccess A function to log success messages
func LogSuccess(ok bool, sign string, id int, msg string) {
	if ok == true {
		LogMsg(sign, id, msg)
	}
}

// LogErr A function to log error messages
func LogErr(err error, sign string, id int, msg string) {
	if err != nil {
		LogMsg(sign, id, msg)
	}
}
