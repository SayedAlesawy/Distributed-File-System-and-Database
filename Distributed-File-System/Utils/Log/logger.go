package logger

import "log"

// LogInfo Used to customize logging info insider generic functions
type LogInfo struct {
	Success string //The message in case of success
	Error   string //The message in case of failure
}

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

// LogDBMsg A function to log messages
func LogDBMsg(sign string, msg string) {
	log.Println(sign, "#", msg)
}

// LogDBSuccess A function to log success messages
func LogDBSuccess(err error, sign string, msg string) {
	if err == nil {
		LogDBMsg(sign, msg)
	}
}

// LogDBErr A function to log error messages
func LogDBErr(err error, sign string, msg string, abort bool) {
	if err != nil {
		LogDBMsg(sign, msg)
		if abort == true {
			panic(err)
		}
	}
}
