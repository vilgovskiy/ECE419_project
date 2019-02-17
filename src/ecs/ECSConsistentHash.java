package ecs;

import com.google.gson.Gson;
import org.apache.log4j.Logger;

import java.util.SortedMap;
import java.util.TreeMap;

public class ECSConsistentHash {

    private SortedMap<String, ECSNode> ring = new TreeMap<>();

    private Logger logger = Logger.getRootLogger();

    public ECSConsistentHash(){
    }

    public void addNode(ECSNode node){
        String nodeHash = node.getNodeHash();
        ECSNode prevNode = findPrevNode(nodeHash);
        node.setPrev(prevNode);

        //When there is only 1 element in the ring, need to set its prev to newly inserted
        if(ring.size() == 1){
            ring.get(ring.lastKey()).setPrev(node);
        }

        

        ring.put(nodeHash, node);
    }

    private ECSNode findPrevNode(String hash){
        ECSNode prevNode = null;
        SortedMap<String, ECSNode> prevNodes = ring.headMap(hash);
        if (!prevNodes.isEmpty()){
            prevNode = prevNodes.get(prevNodes.lastKey());
        } else if (!(prevNodes = ring.tailMap(hash)).isEmpty()){
            prevNode = prevNodes.get(prevNodes.lastKey());
        }
        return null;
    }

    public Integer getRingSize(){return ring.size();}

    public ECSNode removeNode(String key){
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
        ring.putAll(ringRebuilt);

    }



}
