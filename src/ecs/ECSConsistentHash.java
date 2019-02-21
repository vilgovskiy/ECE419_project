package ecs;

import com.google.gson.Gson;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.SortedMap;
import java.util.TreeMap;

public class ECSConsistentHash {

    private SortedMap<String, IECSNode> ring = new TreeMap<>();

    private Logger logger = Logger.getRootLogger();

    public ECSConsistentHash(){
    }

    public ECSConsistentHash(String input){
        updateConsistentHash(input);
    }

    public void addNode(IECSNode node){
        String nodeHash = node.getNodeHash();

        //Find and set previous node for newly added one
        IECSNode prevNode = findPrevNode(nodeHash);
        if (prevNode != null) {
            node.setPrev(prevNode.getNodeHash());
        }

        //Find next node and set new one as previous
        IECSNode nextNode = findNextNode(nodeHash);
        if (nextNode != null) {
            nextNode.setPrev(node.getNodeHash());
        }

        ring.put(nodeHash, node);
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
        if (!nextNodes.isEmpty()){
            nextNode = nextNodes.get(nextNodes.firstKey());
        } else if (!(nextNodes = ring.headMap(hash)).isEmpty()){
            nextNode = nextNodes.get(nextNodes.firstKey());
        }
        return nextNode;
    }

    public Integer getRingSize(){return ring.size();}

    public IECSNode removeNode(String key){
        return  ring.remove(key);
    }

    public void removeAllNodes(){
        ring.clear();
    }

    public boolean empty(){
        return ring.isEmpty();
    }

    public String serializeHash (){
        Gson gson = new Gson();
        String json = gson.toJson(ring);
        return json;
    }

    public void updateConsistentHash(String json){
        ring.clear();
        Gson gson = new Gson();
        Object result = gson.fromJson(json, SortedMap.class);
        SortedMap<String, ECSNode> ringRebuilt = (SortedMap<String, ECSNode>) result;
        for(Map.Entry<String, ECSNode> entry : ringRebuilt.entrySet()){
            ECSNode node = (ECSNode) gson.fromJson(gson.toJson(entry.getValue()), ECSNode.class);
            ring.put(entry.getKey(), node);
        }
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
        ECSNode currNode = (ECSNode) ring.get(upperBound);

        if (ring.size() == 1) {
            // only one node in hash ring, return it
            return currNode;
        } else if (upperBound.compareTo(keyHash) == 0) {
            // if keyHash == node's Hash, then return since it's inclusive
            return currNode;
        } else {
            return (ECSNode) ring.get(currNode.getNodeHash());
        }
    }



}
