package ecs;


import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.*;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ECS implements Watcher {

    private static Logger logger = Logger.getRootLogger();

    public static final String ZK_IP = "127.0.0.1";
    public static final String ZK_PORT = "49999";
    public static final Integer ZK_TIMEOUT = 5000;


    // Set of all servers available in the system through config file
    private Queue<IECSNode> nodePool = new ConcurrentLinkedQueue<>();
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

//        updateMetadata

    }

    @Override
    public void process(WatchedEvent event) {
        if (!event.getState().equals(Event.KeeperState.SyncConnected)){
            logger.error("Error! ZooKeeper connection expired!");

        }

    }
}
