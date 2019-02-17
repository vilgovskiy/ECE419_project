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
        ring.put(nodeHash, node);
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

    public void updateConsistentHashWithNewMetadata(String json){

        ring.clear();

        Gson gson = new Gson();
        Object result = gson.fromJson(json, SortedMap.class);
        SortedMap<String, ECSNode> ringRebuilt = (SortedMap<String, ECSNode>) result;
        ring.putAll(ringRebuilt);

    }



}
