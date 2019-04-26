package fileutils

import (
	"io"
	"log"
	"os"
	"strconv"
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

// DeleteFile A function to delete a file
func DeleteFile(fileName string) {
	err := os.Remove(fileName)
	logErr(err)
}

// AssembleFile A function to assemble pieces into a single file
func AssembleFile(outputFileName string, pieceName string, blockCount int) {
	outFile := CreateFile(outputFileName)
	defer outFile.Close()

	for i := 1; i <= blockCount; i++ {
		fileName := pieceName + "#" + strconv.Itoa(i) + ".mp4"
		count := GetChunksCount(fileName)
		file := OpenFile(fileName)

		for j := 1; j <= count; j++ {
			data, _, _ := ReadChunk(file)
			WriteChunk(outFile, data)
			log.Println("Assembled chunk #", j)
		}

		file.Close()
		DeleteFile(fileName)
		log.Println("Assembled Block #", i)
	}
	log.Println("Assembly Finished")
}
