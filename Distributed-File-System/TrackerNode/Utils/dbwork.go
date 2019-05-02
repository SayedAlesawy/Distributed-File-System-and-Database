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
