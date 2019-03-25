package ecs;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.log4j.Logger;

import java.util.*;

public class ECSConsistentHash {
    private Logger logger = Logger.getRootLogger();
    private static final int REPLICATION_NUMBER = 2;

    private SortedMap<String, IECSNode> ring = new TreeMap<>();

    public ECSConsistentHash(){ }

    public ECSConsistentHash(String input){
        updateConsistentHash(input);
    }

    public void addNode(IECSNode node){
        String nodeHash = node.getNodeHash();
        logger.debug("HashRing adding node " + node.getNodeName() + " with hash " + nodeHash);

        ring.put(nodeHash, node);

        //Find and set previous node for newly added one
        IECSNode prevNode = findPrevNode(nodeHash);
        if (prevNode != null) {
            node.setPrev(prevNode.getNodeHash());
            logger.debug("Node" + node.getNodeName() +
                    "'s prevNode: " + prevNode.getNodeName() + " with hash " + prevNode.getNodeHash());
        }

        //Find next node and set new one as previous
        IECSNode nextNode = findNextNode(nodeHash);
        if (nextNode != null) {
            nextNode.setPrev(node.getNodeHash());
            logger.debug("Node " + node.getNodeName() +
                    "'s nextNode: " + nextNode.getNodeName() + " with hash " + nextNode.getNodeHash());
        }
    }

    private IECSNode findPrevNode(String hash){
        IECSNode prevNode = null;
        SortedMap<String, IECSNode> prevNodes = ring.headMap(hash);
        if (!prevNodes.isEmpty()){
            prevNode = prevNodes.get(prevNodes.lastKey());
        } else if (!(prevNodes = ring.tailMap(hash)).isEmpty()){
            prevNode = prevNodes.get(prevNodes.lastKey());
        }
        return prevNode;
    }

    public IECSNode findNextNode(String hash){
        IECSNode nextNode = null;
        SortedMap<String, IECSNode> nextNodes = ring.tailMap(hash);
        if (nextNodes.size() > 1){
            nextNode = nextNodes.get(nextNodes.keySet().toArray()[1]);
        } else if (!(nextNodes = ring.headMap(hash)).isEmpty()){
            nextNode = nextNodes.get(nextNodes.firstKey());
        }
        return nextNode;
    }

    public Integer getRingSize(){
        return ring.size(); }

    public IECSNode removeNode(String key){
        return  ring.remove(key);
    }

    public void removeAllNodes(){
        ring.clear();
    }

    public boolean empty(){
        return ring.isEmpty();
    }

    public String serializeHashRing () {
        return new Gson().toJson(ring);
    }

    public String serializeHashRingPretty () {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(ring);
    }

    public void updateConsistentHash(String json){
        ring.clear();
        Gson gson = new Gson();
        Object result = gson.fromJson(json, SortedMap.class);
        SortedMap<String, ECSNode> ringRebuilt = (SortedMap<String, ECSNode>) result;
        for(Map.Entry<String, ECSNode> entry : ringRebuilt.entrySet()){
            ECSNode node = gson.fromJson(gson.toJson(entry.getValue()), ECSNode.class);
            ring.put(entry.getKey(), node);
        }
        logger.info("Update hash ring to " + ringRebuilt.toString());
    }

    public ECSNode getNodeByKeyHash(String keyHash){
        // get the node that has hashValue larger or equal than keyHash
        SortedMap<String, IECSNode> greaterThanOrEq = ring.tailMap(keyHash);
        String upperBound;

        if (greaterThanOrEq.isEmpty())  {
            // if the hashed value is greater than all hash values, loop back to start
            upperBound = ring.firstKey();
        } else {
            upperBound = greaterThanOrEq.firstKey();
        }
        return (ECSNode) ring.get(upperBound);

//        if (ring.size() == 1) {
//            // only one node in hash ring, return it
//            return currNode;
//        } else if (upperBound.compareTo(keyHash) == 0) {
//            // if keyHash == node's Hash, then return since it's inclusive
//            return currNode;
//        } else {
//            return (ECSNode) ring.get(currNode.getPrevNode());
//        }
    }

    public ECSNode getNodeByNodeName(String nodeName) {
        if (ring.isEmpty()) return null;

        // loop through all the nodes in ring
        for (IECSNode node : ring.values()) {
            String name = node.getNodeName();
            // found
            if (name.equals(nodeName)) {
                return (ECSNode) node;
            }
        }
        // cannot find the node by name
        logger.warn("Cannot get node from hash ring with nodeName " + nodeName);
        return null;
    }

    // find the next two successor of the coordinator node
    public Set<IECSNode> getReplicaNodesByCoordinator(IECSNode coordinator) {
        assert ring.size() > REPLICATION_NUMBER;

        // use set to prevent duplicates
        Set<IECSNode> replicaSet = new HashSet<>();
        IECSNode curr = coordinator;

        // use findNextNode to find the N successors after coordinator
        for (int i = 0 ; i < REPLICATION_NUMBER; i++) {
            IECSNode nextNode = findNextNode(curr.getNodeHash());
            // assert that the nextNode's previous node is the curr node
            assert nextNode.getPrevNode().equals(curr.getNodeHash());
            replicaSet.add(nextNode);
            curr = nextNode;
        }

        logger.debug("Replica coordinator " + coordinator.getNodeName()
                + ", replica nodes" + replicaSet.toString());
        return replicaSet;
    }


}
