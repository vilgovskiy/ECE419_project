package server;

import com.sun.security.ntlm.Server;
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
    private IECSNode node;
    private List<ServerReplicator> replicatorList;

    public ReplicationManager(String nodeName, String nodeHost, int nodePort) {
        this.node = new ECSNode(nodeName, nodeHost, nodePort);
        this.replicatorList = new ArrayList<>();
    }

    public void updateReplicatorList(ECSConsistentHash consistentHash) throws IOException {
        IECSNode coordinatorNode = consistentHash.getNodeByNodeName(node.getNodeName());
        Set<IECSNode> replicaSet = consistentHash.getReplicaNodesByCoordinator(coordinatorNode);

        List<ServerReplicator> newReplicatorList = new ArrayList<>();
        for (IECSNode replicaNode : replicaSet) {
            ServerReplicator replicator = new ServerReplicator(replicaNode);
            newReplicatorList.add(replicator);
        }

        for (ServerReplicator replicator : replicatorList) {
            if (!newReplicatorList.contains(replicator)) {
                replicator.close();
                replicatorList.remove(replicator);
            }
        }

        for (ServerReplicator replicator : newReplicatorList) {
            if (!replicatorList.contains(replicator)) {
                replicator.connect();
                replicatorList.add(replicator);
            }
        }
    }

    public void replicate(KVMessage message) throws Exception {
        for (ServerReplicator replicator : replicatorList) {
            replicator.replicatePut(message);
        }
    }

    public void clearReplicatorList() {
        for (ServerReplicator replicator : replicatorList) {
            replicator.close();
        }
        replicatorList.clear();
    }

}
