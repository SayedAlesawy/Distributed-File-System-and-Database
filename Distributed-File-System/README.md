#  Distributed File System Port Map
This detials the port map used by the different submodules within the Distributed File System module.

## Tracker Ports
The launcher process in the Tracker module spwans 2 types of processes:
- **The Master Tracker**

    ```
    - Receiving the IPs for all Data Nodes in the system
    - Receiving the heartbeat signals from the launcher module of each data node
    - Updating the Alive Data Nodes in the Database.
    ```
    **Ports:**
    - Port 9000 for the Master Tracker
- **The Side Tracker Processes**

    These processes are responsible for the Tracker job
    ```
    - Listening to Client requests.
    - Communicating with Data Nodes.
    - Updating the meta file Database.
    ```
    - Ports 9001 and 9002 for the side Tracker processes communications with Data Nodes.
    - Ports 8001 and 8002 for the side Tracker processes communications with Clients.

## Data Node Ports
The lanucher process in the Data Node module spwans 2 types of processes:
- **The Heartbeat Node**

    process
    ```
    - Sending the Heartbeat Port to the Tracker.
    - Sending the Data Node IPs.
    - Sending the Data Node machine ID.
    - Sending the heartbeat signals. 
    ```
    Here, we use a base port, and based upon we derive the other ports as follows:
    If the base port of a Data Node machine is 70 for example, then:
    - Port 7000 for the launcher Data Node (Heart beat).
    - Ports 7011, 7021 for the request ports.
    - Ports 7012, 7022 for the upload ports.
    - Ports 7013, 7023 for the download ports.
    - Ports 7014, 7024 for the replication upload ports.
    - Ports 7015, 7025 for the replication download ports.

# Testing Data Set
The testing data set can be found [here](https://drive.google.com/drive/folders/1pEVD85lamr6kkhFnDCPDCZFO5JPk7Ajd?usp=sharing)