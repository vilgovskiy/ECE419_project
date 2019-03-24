package server;

import ecs.ECSConsistentHash;
import ecs.IECSNode;
import ecs.ECSNode;
import shared.messages.*;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;


public class ReplicationManager {
    private static Logger logger = Logger.getRootLogger();
    private IECSNode coordinator;
    private List<ServerReplicator> replicationList;

    public ReplicationManager(String nodeName, String nodeHost, int nodePort) {
        this.coordinator = new ECSNode(nodeName, nodeHost, nodePort);
        this.replicationList = new ArrayList<>();
    }

    // update replication list based on new hashring info
    public void updateReplicatorList(ECSConsistentHash consistentHash) throws IOException {
        // get coordinator and replicas from hash ring
        IECSNode coordinatorNode = consistentHash.getNodeByNodeName(coordinator.getNodeName());
        Set<IECSNode> replicaSet = consistentHash.getReplicaNodesByCoordinator(coordinatorNode);

        String logMsg = "Setting up replication. Coordinator: " + coordinator.getNodeName() + " Replicas: ";
        for (IECSNode node: replicaSet) {
            logMsg += " " + node.getNodeName() + " ";
        }
        logger.info(logMsg);

        // create updated replication list
        List<ServerReplicator> newReplicationList = new ArrayList<>();
        for (IECSNode replicaNode : replicaSet) {
            ServerReplicator replicator = new ServerReplicator(replicaNode);
            newReplicationList.add(replicator);
        }

        // if replicator in old replicationlist is not in the updated one
        // close replicator's connection
        for (ServerReplicator replicator : replicationList) {
            if (!newReplicationList.contains(replicator)) {
                replicator.close();
                replicationList.remove(replicator);
            }
        }

        // add new replicator to replicationList
        for (ServerReplicator replicator : newReplicationList) {
            if (!replicationList.contains(replicator)) {
                replicator.connect();
                replicationList.add(replicator);
            }
        }
    }

    // send REPLICA_PUT message to replica nodes
    public void sendReplicaPuts(KVMessage message) throws Exception {
        logger.info("Sending replica Puts with coordinator " + coordinator.getNodeName());
        for (ServerReplicator replicator : replicationList) {
            logger.info("Sending replica puts to " + replicator.getNodeHost() + ":" + replicator.getNodePort());
            replicator.replicaPut(message);
        }
    }

    public void clearReplicatorList() {
        for (ServerReplicator replicator : replicationList) {
            replicator.close();
        }
        replicationList.clear();
    }

}
