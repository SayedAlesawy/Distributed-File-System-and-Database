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
