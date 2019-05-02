package trackernode

// dataNodeRow Represents a data node row in the Database
type dataNodeRow struct {
	id       int
	ip       string
	basePort string
}

type fileRow struct {
	fileName string
	clientID string
	fileSize int
	location string
}
