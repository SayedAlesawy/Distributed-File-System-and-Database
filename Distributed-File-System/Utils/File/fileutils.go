package fileutils

import (
	"io"
	"log"
	"os"
)

// LogSign Used in logging FileUtil errors
const LogSign string = "[File]"

// ChunkSize The chunk size for reading and sending files
const ChunkSize int64 = 1024 * 1024

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

// GetFileSize A function to return the file size in bytes
func GetFileSize(fileName string) int64 {
	fileInfo, err := os.Stat(fileName)
	logErr(err)

	return fileInfo.Size()
}

// SeekPosition A function to seek a certian position in a file
func SeekPosition(file *os.File, start int) *os.File {
	offset := int64(start) * ChunkSize
	whence := 0

	file.Seek(offset, whence)

	return file
}

// OpenSeekFile A function to open a file from a certain start
func OpenSeekFile(fileName string, start int) *os.File {
	file := OpenFile(fileName)
	file = SeekPosition(file, start)

	return file
}

// GetChunksCount A function to get the number of chunks in a file
func GetChunksCount(fileName string) int {
	fileSize := GetFileSize(fileName)
	chunksCount := (fileSize + ChunkSize - 1) / ChunkSize

	return int(chunksCount)
}

// ReadChunk A function to read chunkSize bytes of a file from the previous position
func ReadChunk(file *os.File) ([]byte, int, bool) {
	buffer := make([]byte, ChunkSize)
	size, err := file.Read(buffer)

	done := isDone(err)

	return buffer, size, done
}

// WriteChunk A function to write chunkSize bytes to a file
func WriteChunk(file *os.File, chunk []byte) int {
	size, err := file.Write(chunk)
	logErr(err)

	return size
}
