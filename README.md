# **Distributed File System**
The main objective of the File System is to offer a platform for data storage, that's **distributed** on multiple machines, **highly available**, **reliable** and **fault tolerant**. 

## **Table of Contents**
- [**System Components**](#system-components)
    * [**Tracker Node**](#tracker-node)
    * [**Data Keeper Node**](#data-keeper-node)
- [**Data Replication Mechanism**](#data-replication-mechanism)
- [**Types of Requests**](#types-of-requests)
- [**Request Handling**](#request-handling)
    * [**Upload Request Handler**](#upload-request-handler)
    * [**Download Request Handler**](#download-request-handler)
    * [**Display Request Handler**](#display-request-handler)
- [**Fault Tolerance**](#fault-tolerance)

## **System Components**
The system consists of 2 types of **Nodes**, namely, the **Tracker Node** and the **Data Keeper Node**.

- ### **Tracker Node**
    The Tracker Node is a multi-process node that works as the coordinator of the system. The following highlights its main functionalities:
    - Receives handshake signals from Data Keeper Nodes and starts to track them.
    - Keeps track of all currently alive Data Keeper Nodes through receiving a periodic (every 1 sec) heartbeat signals and updating its internal tracking structure accordingly. The criteria is that if any Data Keeper Node missed its heartbeat window (2 secs), it will be considered dead.
    - Receives user requests (upload/download/display) and handles them accordingly.
    - Initiates a Replication routine that replicates data on multiple machines for higher reliability.
    - Keeps a Database of all the files stored on the File System.

- ### **Data Keeper Node**
    The Data Keeper Node is multi-process node that works as the storage utility of the system. The following highlights its main functionalities:
    - Sending handshake signals to the Tracker Node to establish communication with it.
    - Sending periodic heartbeat signals to inform the Tracker Node of its status.
    - Receives files from users in case of handling an upload request and manages internal users' directories.
    - Sends files to users in case of handling a download request.
    - Receives and sends files to other Data Keeper Nodes in case of handling a Replication command from the Tracker Node.

## **Data Replication Mechanism**
File System does Data Replication to increase its reliability and availability. The system aims for a 3/n replication ratio, meaning that every file that's uploaded to the file system is replicated 3 times across the system. The replication also enables the system to provide a multi-source download to users that speeds up the download process and stops the server capacity from being a bottleneck during download.

The Replication is a periodic routine that initiated by the Tracker Node once every 2 mins (parameter, can be changed). The Replication routine goes as follows:
- The Tracker Node refers to its database and determines the files that need replication.
- The Tracker Node chooses a suitable source Data Keeper Node, and a suitable destination Data Keeper Node for replication.
- The Tracker Node sends a replication request to the source Data Keeper Node and provides the needed information about the destination Data Keeper Node.
- The source Data Keeper Node establishes communication with the destination Node and the file replication starts.
- Once the replication is done, the source Data Keeper Node sends a completion confirmation to the Tracker Node so it can update its database.

## **Types of Requests**
The system supports 3 types of requests:
- An Upload request.
- A download request.
- A display request (analogous to the `ls` command in UNIX).


## **Request Handling**
The system handles the 3 formerly mentioned types of requests in the following fashion:
- ### **Upload Request Handler**

    The upload request handler works as follows:
    - The authenticated user sends an upload request to the Tracker Node.
    - The Tracker Node refers to its Database and selects a Data Keeper Node for the file transfer.
    - The Tracker Node sends back the IP:Port combination to the client software in order to establish communication with the user.
    - The users re-sends the request to the designated Data Keeper Node and established connection.
    - The Data Keeper Node starts receiving data from the user 1 chunk (1 MB) at a time, and stores the data into the user's directory.
    - When the file transfer is finished, the Data Keeper Node sends a completion confirmation to the Tracker Node.
    - The Tracker Node updates its files database and sends a completion confirmation back to the user.

    Notes: 
    - The process of upload request handling is completely multi-threaded. A single Data Keeper Node can handle virtually infinite number of upload requests (only bounded by the network capacity) at the same time, but the performance tends to suffer when the number of requests being served is high.
    - The data upload is done on chunk basis, so if the connection was interrupted, the receiving can still resume from the point it has left off.

- ### **Download Request Handler**  

    The download request handler works as follows:
    - The authenticated user sends a download request to the Tracker Node.
    - The Tracker Nodes verifies if the user has access to the desired file and terminates if the user doesn't have access rights.
    - If user has access rights, the Tracker Node refers to its database to determine all the locations of the file (Every Data Keeper Node that has a copy of this file, whether through direct upload, or replication).
    - The Tracker Nodes prepares the IP:Port combination of all Data Keeper Nodes that are ready to provide the file and sends them back to the user along with the file size.
    - The user's software uses the file size to divide the file download on all the available Data Keeper Nodes.
    - The user's software dispatches a thread for each file block, each threads sends a request to its corresponding Data Keeper Node, starts to received its designated block of the file and temporarily writes it on the disk.
    - The user's software waits for all threads to terminate, and then it combines all the downloaded pieces by all threads into a single file on the user's machine.

    Notes:
    - The process of download request handling is completely multi-threaded. A single Data Keeper Node can handle virtually infinite number of download requests (only bounded by the network capacity) at the same time, but the performance tends to suffer when the number of requests being served is high.
    - The data download process is done on chunk basis, so if the connection was interrupted, the download can still continue by only ordering the chunks that are not currently present on the disk [Not implemented yet].
    - The file assembly process is done offline, so if the user was disconnected, the assembly would still work. 
    - The download process is no longer limited by the server's capacity, because the download is taking place form multiple different servers simultaneously. So the only bottleneck becomes the user's network capacity.

- ### **Display Request Handler**

    The display request handler works as follows:
    - The authenticated user sends a display request to the Tracker Node.
    - The Tracker Node refers to its database and fetches all the files owned by the current user.
    - The Tracker Node serializes the response and sends it back to the user.

**Note**: Due to the multi-threaded, multi-processing nature of the system, a single Node can serve any number of any type of request all at the same time.

## **Fault Tolerance**
The system is designed with fault tolerance in mind. The system is able to identify and handle the following types of faults:
- User - Tracker Node connection drop.
- User - Data Keeper Node connection drop.
- Data Keeper Node - Data Keeper Node connection drop.
- Tracker - Data Keeper Node connection drop.

The system identifies these types of faults by running blocking functions (send/receive) on different threads and uses channels to notify the main thread of completion. The main thread makes sure that any thread that doesn't terminate before a preset timeout is gracefully handled and forcibly terminated.

The system is also able to identify other types of errors such as:
- Wrong file names in upload requests.
- Unauthorized access of files.
