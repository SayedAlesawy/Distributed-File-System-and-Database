package trackernode

import (
	comm "Distributed-Video-Processing-Cluster/Distributed-File-System/Utils/Comm"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pebbe/zmq4"
)

// ListenToHeartbeats A function to listen to incoming heartbeats
func (trackerNodeLauncherObj *trackerNodeLauncher) ListenToHeartbeats(HBIPsMutex *sync.Mutex, timeStampsMutex *sync.Mutex) {
	trackerNodeLauncherObj.establishSubscriberConnection()

	defer trackerNodeLauncherObj.subscriberSocket.Close()

	for {
		trackerNodeLauncherObj.updateSubscriberConnection(HBIPsMutex)

		heartbeat, _ := trackerNodeLauncherObj.subscriberSocket.Recv(zmq4.DONTWAIT)

		if heartbeat != "" {
			log.Println(LogSignL, "Received", heartbeat)

			trackerNodeLauncherObj.registerTimeStap(heartbeat, timeStampsMutex)
		}
	}
}

// registerTimeStap A function to register the timestamp of the last received heartbeat
func (trackerNodeLauncherObj *trackerNodeLauncher) registerTimeStap(heartbeat string, timeStampMutex *sync.Mutex) {
	id, _ := strconv.Atoi((strings.Fields(heartbeat))[1])

	timeStampMutex.Lock()
	trackerNodeLauncherObj.datanodeTimeStamps[id] = time.Now()
	timeStampMutex.Unlock()
}

// updateDataNodeAliveStatus A function the update the status of the alive datanodes
func (trackerNodeLauncherObj *trackerNodeLauncher) UpdateDataNodeAliveStatus(HBIPsMutex *sync.Mutex, DNIPsMutex *sync.Mutex, timeStampsMutex *sync.Mutex) {
	for {
		timeStampsMutex.Lock()

		for id, timestamp := range trackerNodeLauncherObj.datanodeTimeStamps {
			diff := time.Now().Sub(timestamp)
			threshold := trackerNodeLauncherObj.disconnectionThreshold

			if diff > threshold {
				connection := []string{"tcp://" + trackerNodeLauncherObj.datanodeHBIPs[id]}
				comm.Disconnect(trackerNodeLauncherObj.subscriberSocket, connection)

				HBIPsMutex.Lock()
				delete(trackerNodeLauncherObj.datanodeHBIPs, id)
				HBIPsMutex.Unlock()

				DNIPsMutex.Lock()
				delete(trackerNodeLauncherObj.datanodeIPs, id)
				DNIPsMutex.Unlock()

				delete(trackerNodeLauncherObj.datanodeTimeStamps, id)

				log.Println(LogSignL, "Node#", id, "has gone offline")
			}
		}

		timeStampsMutex.Unlock()
	}
}
