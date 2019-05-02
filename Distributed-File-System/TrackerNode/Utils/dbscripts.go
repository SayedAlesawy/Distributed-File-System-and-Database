package trackernode

// sqlCreateDataNodesTable SQL to create the DataNodes table
const sqlCreateDataNodesTable string = `
	CREATE TABLE datanodes (
	id SERIAL PRIMARY KEY,
	dataNodeID int UNIQUE NOT NULL,
	ip varchar(60) NOT NULL,
	basePort varchar(60) NOT NULL
	);
`

// sqlCreateMetaFile SQL to create the Meta Files table
const sqlCreateMetaFile string = `
	CREATE TABLE metafiles (
	id SERIAL PRIMARY KEY,
	fileName varchar(60) NOT NULL,
	clientID int NOT NULL,
	fileSize int NOT NULL, 
	location int NOT NULL
	);
	ALTER TABLE metafiles
	ADD CONSTRAINT unq_filename_clientid UNIQUE(fileName, clientID);
`

// sqlDropDataNodesTable SQL to drop the DataNodes table
const sqlDropDataNodesTable string = `DROP TABLE IF EXISTS datanodes;`

// sqlDropMetaFileTable SQL to drop the Meta Files table
const sqlDropMetaFileTable string = `DROP TABLE IF EXISTS metafiles;`

// sqlInsertDataNode SQL to insert a data node in the DataNodes table
const sqlInsertDataNode string = `
	INSERT INTO datanodes (dataNodeID, ip, basePort)
	VALUES ($1, $2, $3)
`

// sqlInsertFileEntry SQL to insert a a file entry into the Meta File table
const sqlInsertFileEntry string = `
	INSERT INTO metafiles (fileName, clientID, fileSize, location)
	VALUES ($1, $2, $3, $4)
`

// sqlDeleteDataNode SQL to delete a data node from the DataNodes table
const sqlDeleteDataNode string = `DELETE FROM datanodes WHERE dataNodeID=$1`
