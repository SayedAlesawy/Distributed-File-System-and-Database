package trackernode

import (
	comm "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Comm"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/pebbe/zmq4"
)

// ListenToHeartbeats A function to listen to incoming heartbeats
func (trackerNodeLauncherObj *trackerNodeLauncher) ListenToHeartbeats() {
	trackerNodeLauncherObj.establishSubscriberConnection()

	defer trackerNodeLauncherObj.subscriberSocket.Close()

	for {
		trackerNodeLauncherObj.updateSubscriberConnection()

		heartbeat, _ := trackerNodeLauncherObj.subscriberSocket.Recv(zmq4.DONTWAIT)

		if heartbeat != "" {
			log.Println(LogSignL, "Received", heartbeat)

			trackerNodeLauncherObj.registerTimeStap(heartbeat)
		}
	}
}

// registerTimeStap A function to register the timestamp of the last received heartbeat
func (trackerNodeLauncherObj *trackerNodeLauncher) registerTimeStap(heartbeat string) {
	id, _ := strconv.Atoi((strings.Fields(heartbeat))[1])

	trackerNodeLauncherObj.timeStampMutex.Lock()
	trackerNodeLauncherObj.datanodeTimeStamps[id] = time.Now()
	trackerNodeLauncherObj.timeStampMutex.Unlock()
}

// updateDataNodeAliveStatus A function the update the status of the alive datanodes
func (trackerNodeLauncherObj *trackerNodeLauncher) UpdateDataNodeAliveStatus() {
	for {
		trackerNodeLauncherObj.timeStampMutex.Lock()

		for id, timestamp := range trackerNodeLauncherObj.datanodeTimeStamps {
			diff := time.Now().Sub(timestamp)
			threshold := trackerNodeLauncherObj.disconnectionThreshold

			if diff > threshold {
				connection := []string{comm.GetConnectionString(trackerNodeLauncherObj.datanodeIPs[id], trackerNodeLauncherObj.datanodeBasePorts[id]+"00")}
				comm.Disconnect(trackerNodeLauncherObj.subscriberSocket, connection)

				trackerNodeLauncherObj.ipsMutex.Lock()
				delete(trackerNodeLauncherObj.datanodeIPs, id)
				trackerNodeLauncherObj.ipsMutex.Unlock()

				trackerNodeLauncherObj.portsMutex.Lock()
				delete(trackerNodeLauncherObj.datanodeBasePorts, id)
				trackerNodeLauncherObj.portsMutex.Unlock()

				delete(trackerNodeLauncherObj.datanodeTimeStamps, id)

				trackerNodeLauncherObj.dbMutex.Lock()
				deleteDataNode(trackerNodeLauncherObj.db, id)
				trackerNodeLauncherObj.dbMutex.Unlock()

				log.Println(LogSignL, "Node#", id, "has gone offline")
			}
		}

		trackerNodeLauncherObj.timeStampMutex.Unlock()
	}
}
