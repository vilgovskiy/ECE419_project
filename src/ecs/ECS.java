package ecs;


import app_kvECS.IECSClient;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ECS implements IECSClient {

    private static Logger logger = Logger.getRootLogger();

    public static final String ZK_IP = "127.0.0.1";
    public static final String ZK_PORT = "49999";
    public static final Integer ZK_TIMEOUT = 5000;

    public static final String ZK_METADATA_PATH = "/metadata";
    public static final String ZK_SERVER_ROOT = "/kv_servers";

    // Set of all servers available in the system through config file
    private Queue<IECSNode> nodePool = new ConcurrentLinkedQueue<>();

    //Holds all currently initialized nodes
    private Map<String, IECSNode> initNodes = new HashMap<>();

    //Contains all all hash mappings of all currently active servers
    private ECSConsistentHash hashRing = new ECSConsistentHash();
    private ZooKeeper zk;

    public ECS(String configFile) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(new File(configFile)));

        String cur;

        while((cur = br.readLine()) != null){
            String[] tokens = cur.split(" ");
            ECSNode node = new ECSNode(tokens[0], tokens[1], Integer.parseInt(tokens[2]));

            nodePool.add(node);
        }

        zk = new ZooKeeper(ZK_IP + ":" + ZK_PORT, ZK_TIMEOUT, this);

        updateMetadata();

    }

    @Override
    public void process(WatchedEvent event) {
        if (!event.getState().equals(Event.KeeperState.SyncConnected)){
            logger.error("Error! ZooKeeper connection expired!");

        }

    }

    @Override
    public boolean start(){
        //Assume that servers are already initialized and currently in STOPPED state
        List<IECSNode> listOfNodesToStart = new ArrayList<>();
        for(IECSNode node : initNodes.values()){
            if (node.getStatus().equals(ECSNode.ServerStatus.STOP)){
                listOfNodesToStart.add(node);
            }
        }
        for(IECSNode node : listOfNodesToStart){
            hashRing.addNode(node);
        }

        //Need to first redistribute all data among the nodes

        //Then need to activate all the nodes
        return true;
    }








    /**
     * Push the metadata(hash ring content) to ZooKeeper z-node
     */
    private boolean updateMetadata() {
        try {
            Stat exists = zk.exists(ZK_METADATA_PATH, false);
            if (exists == null) {
                zk.create(ZK_METADATA_PATH, hashRing.serializeHash().getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } else {
                zk.setData(ZK_METADATA_PATH, hashRing.serializeHash().getBytes(),
                        exists.getVersion());
            }
        } catch (InterruptedException e) {
            logger.error("Interrupted");
            return false;
        } catch (KeeperException e) {
            logger.error(e.getMessage());
            return false;
        }
        return true;
    }
}
