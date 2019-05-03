package trackernode

import (
	dbwrapper "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Database"
	logger "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Log"
	"fmt"

	"database/sql"
)

// insertDataNode A function to insert a data node in the DataNodes table
func insertDataNode(db *sql.DB, dataNodeID int, ip string, basePort string) bool {
	sqlStatement := sqlInsertDataNode

	logMsgs := logger.LogInfo{
		Success: fmt.Sprintf("DataNode #%d created Successfully", dataNodeID),
		Error:   fmt.Sprintf("Failed to create DataNode #%d", dataNodeID),
	}

	ok := dbwrapper.ExecuteQuery(db, sqlStatement, logMsgs, false, dataNodeID, ip, basePort)

	return ok
}

// insertMetaFile A function to insert a meta file entry into the Database
func insertMetaFile(db *sql.DB, fileName string, clientID int, filseSize int, location string) bool {
	sqlStatement := sqlInsertFileEntry

	logMsgs := logger.LogInfo{
		Success: fmt.Sprintf("Successfully insert file: %s of client %d", fileName, clientID),
		Error:   fmt.Sprintf("Failed to insert file: %s of client %d", fileName, clientID),
	}

	ok := dbwrapper.ExecuteQuery(db, sqlStatement, logMsgs, false, fileName, clientID, filseSize, location)

	return ok
}

// deleteDataNode A function to delete a data node from the DataNodes table
func deleteDataNode(db *sql.DB, dataNodeID int) bool {
	sqlStatement := sqlDeleteDataNode

	logMsgs := logger.LogInfo{
		Success: fmt.Sprintf("DataNode #%d deleted Successfully", dataNodeID),
		Error:   fmt.Sprintf("Failed to delete DataNode #%d", dataNodeID),
	}

	ok := dbwrapper.ExecuteQuery(db, sqlStatement, logMsgs, false, dataNodeID)

	return ok
}

// selectDatanodes A function to select all datanodes
func selectDatanodes(db *sql.DB) []dataNodeRow {
	sqlStatement := sqlSelectAllDataNodes

	logMsgs := logger.LogInfo{
		Success: "Datanode list selected Successfully",
		Error:   "Datanode list selection failed",
	}

	rows, ok := dbwrapper.ExecuteRowsQuery(db, sqlStatement, logMsgs, false)
	defer rows.Close()

	var datanodeList []dataNodeRow
	for rows.Next() {
		var serialID int
		var dataNodeID int
		var ip string
		var basePort string

		err := rows.Scan(&serialID, &dataNodeID, &ip, &basePort)
		logger.LogDBErr(err, dbwrapper.LogSign, "selectDatanodes(): Error while extracting results", false)

		res := dataNodeRow{
			id:       dataNodeID,
			ip:       ip,
			basePort: basePort,
		}

		datanodeList = append(datanodeList, res)
	}

	err := rows.Err()
	logger.LogDBErr(err, dbwrapper.LogSign, "selectDatanodes(): Error while extracting results", false)
	logger.LogDBSuccess(err, dbwrapper.LogSign, "DataNode list extracted successfully")

	if ok == false {
		datanodeList = []dataNodeRow{}
	}

	return datanodeList
}

// selectDataNode A function to select a datanode
func selectDataNode(db *sql.DB, id int) (dataNodeRow, bool) {
	sqlStatement := sqlSelectDataNode

	row := dbwrapper.ExecuteRowQuery(db, sqlStatement, id)

	var serialID int
	var res dataNodeRow

	err := row.Scan(&serialID, &res.id, &res.ip, &res.basePort)
	if err == sql.ErrNoRows {
		return dataNodeRow{}, false
	}

	return res, true
}

func selectMetaFiles(db *sql.DB) []fileRow {
	sqlStatement := sqlSelectAllMetaFiles

	logMsgs := logger.LogInfo{
		Success: "File list selected Successfully",
		Error:   "File list selection failed",
	}

	rows, ok := dbwrapper.ExecuteRowsQuery(db, sqlStatement, logMsgs, false)
	defer rows.Close()

	var fileList []fileRow
	for rows.Next() {
		var serialID int
		var fileName string
		var clientID int
		var fileSize int
		var location string

		err := rows.Scan(&serialID, &fileName, &clientID, &fileSize, &location)
		logger.LogDBErr(err, dbwrapper.LogSign, "selectMetaFiles(): Error while extracting results", false)

		res := fileRow{
			fileName: fileName,
			clientID: clientID,
			fileSize: fileSize,
			location: location,
		}

		fileList = append(fileList, res)
	}

	err := rows.Err()
	logger.LogDBErr(err, dbwrapper.LogSign, "selectMetaFiles(): Error while extracting results", false)
	logger.LogDBSuccess(err, dbwrapper.LogSign, "File list extracted successfully")

	if ok == false {
		fileList = []fileRow{}
	}

	return fileList
}

// selectMetaFile A function to select a metafile entry
func selectMetaFile(db *sql.DB, fileName string, clientID int) (fileRow, bool) {
	sqlStatement := sqlSelectMetaFile

	row := dbwrapper.ExecuteRowQuery(db, sqlStatement, fileName, clientID)

	var serialID int
	var res fileRow

	err := row.Scan(&serialID, &res.fileName, &res.clientID, &res.fileSize, &res.location)

	if err == sql.ErrNoRows {
		return fileRow{}, false
	}

	return res, true
}

// UpdatePeerDownload A function to update the peer-download status
func updateMetaFile(db *sql.DB, location string, fileName string, clientID int) bool {
	sqlStatement := sqlUpdateMetaFile

	logMsgs := logger.LogInfo{
		Success: "Updated metafile Successfully",
		Error:   "Metafile update failed",
	}

	ok := dbwrapper.ExecuteQuery(db, sqlStatement, logMsgs, false, location, fileName, clientID)

	return ok
}
