package fileutils

import (
	"io"
	"log"
	"os"
)

// LogSign Used in logging FileUtil errors
const LogSign string = "[File]"

// logErr A function to log the error message
func logErr(err error) {
	if err != nil {
		log.Fatal(LogSign, err)
	}
}

// isDone A function to determine when read a file is done
func isDone(err error) bool {
	if err == io.EOF {
		return true
	}
	return false
}

// CreateFile A function to create a file specified by name
func CreateFile(fileName string) *os.File {
	file, err := os.Create(fileName)
	logErr(err)

	return file
}

// OpenFile A function to open a file specified by name
func OpenFile(fileName string) *os.File {
	file, err := os.Open(fileName)
	logErr(err)

	return file
}

// ReadChunk A function to read chunkSize bytes of a file from the previous position
func ReadChunk(file *os.File, chunkSize int) ([]byte, int, bool) {
	byteSlice := make([]byte, chunkSize)
	bytesRead, err := file.Read(byteSlice)

	done := isDone(err)

	return byteSlice, bytesRead, done
}

// WriteChunk A function to write chunkSize bytes to a file
func WriteChunk(file *os.File, chunk []byte) int {
	bytesWritten, err := file.Write(chunk)
	logErr(err)

	return bytesWritten
}
