package ecs;

import com.google.gson.Gson;
import org.apache.log4j.Logger;

import java.util.SortedMap;
import java.util.TreeMap;

public class ECSConsistentHash {

    private final SortedMap<String, ECSNode> ring = new TreeMap<>();

    private Logger logger = Logger.getRootLogger();

    public ECSConsistentHash(){
    }

    public void addNode(ECSNode node){
        String nodeHash = node.getNodeHash();
        ring.put(nodeHash, node);

        String prevNodeKey =  ring.headMap(nodeHash).lastKey();
        ECSNode prevNode = ring.get(prevNodeKey);

    }

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
        return "";
    }



}
