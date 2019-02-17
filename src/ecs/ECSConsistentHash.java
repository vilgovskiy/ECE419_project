package ecs;

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
    }

}
