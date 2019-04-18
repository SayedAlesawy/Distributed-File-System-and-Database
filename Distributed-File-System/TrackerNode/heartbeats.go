package trackernode

import (
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pebbe/zmq4"
)

// ListenToHeartbeats A function to listen to incoming heartbeats
func (heartbeatTrackerNodeObj *heartbeatTrackerNode) ListenToHeartbeats(IPsMutex *sync.Mutex, timeStampsMutex *sync.Mutex) {
	heartbeatTrackerNodeObj.establishSubscriberConnection()

	defer heartbeatTrackerNodeObj.subscriberSocket.Close()

	for {
		heartbeatTrackerNodeObj.updateSubscriberConnection(IPsMutex)

		heartbeat, _ := heartbeatTrackerNodeObj.subscriberSocket.Recv(zmq4.DONTWAIT)

		if heartbeat != "" {
			log.Println("Received", heartbeat)

			heartbeatTrackerNodeObj.registerTimeStap(heartbeat, timeStampsMutex)
		}

		//heartbeatTrackerNodeObj.printMap(IPsMutex)
	}
}

// registerTimeStap A function to register the timestamp of the last received heartbeat
func (heartbeatTrackerNodeObj *heartbeatTrackerNode) registerTimeStap(heartbeat string, timeStampMutex *sync.Mutex) {
	id, _ := strconv.Atoi((strings.Fields(heartbeat))[1])

	timeStampMutex.Lock()
	heartbeatTrackerNodeObj.datanodeTimeStamps[id] = time.Now()
	timeStampMutex.Unlock()
}

// updateDataNodeAliveStatus A function the update the status of the alive datanodes
func (heartbeatTrackerNodeObj *heartbeatTrackerNode) UpdateDataNodeAliveStatus(IPsMutex *sync.Mutex, timeStampsMutex *sync.Mutex) {
	for {
		timeStampsMutex.Lock()

		for id, timestamp := range heartbeatTrackerNodeObj.datanodeTimeStamps {
			diff := time.Now().Sub(timestamp)
			threshold := heartbeatTrackerNodeObj.disconnectionThreshold

			if diff > threshold {
				heartbeatTrackerNodeObj.disconnectSocket(heartbeatTrackerNodeObj.datanodeIPs[id])

				IPsMutex.Lock()
				delete(heartbeatTrackerNodeObj.datanodeIPs, id)
				IPsMutex.Unlock()

				delete(heartbeatTrackerNodeObj.datanodeTimeStamps, id)

				log.Println("[Heartbeat Tracker Node]", "deleting node#", id)
			}
		}

		timeStampsMutex.Unlock()
	}
}

// printMap A debug function to print the current datanodes
func (heartbeatTrackerNodeObj heartbeatTrackerNode) printMap(IPsMutex *sync.Mutex) {
	IPsMutex.Lock()
	for id, ip := range heartbeatTrackerNodeObj.datanodeIPs {
		log.Println("Tracking: ", id, " -- ", ip)
	}
	IPsMutex.Unlock()
}
