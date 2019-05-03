package trackernode

// dataNodeRow Represents a data node row in the Database
type dataNodeRow struct {
	id       int
	ip       string
	basePort string
}

// fileRow Represents a file entry row in the Database
type fileRow struct {
	fileName string
	clientID int
	fileSize int
	location string
}
