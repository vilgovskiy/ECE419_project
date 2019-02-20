package ecs;


import app_kvECS.IECSClient;
import server.ServerMetadata;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import com.google.gson.Gson;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;


public class ECS implements Watcher, IECSClient {

    private static Logger logger = Logger.getRootLogger();

    public static final String ZK_IP = "127.0.0.1";
    public static final String ZK_PORT = "49999";
    public static final Integer ZK_TIMEOUT = 5000;

    public static final String ZK_METADATA_PATH = "/metadata";
    public static final String ZK_SERVER_ROOT = "/kv_servers";
    public static final String SERVER_JAR_PATH = "KVServer.jar";

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

        // Need to broadcast KVadmin message to all nodes

        for (IECSNode node : listOfNodesToStart) {
            node.setStatus(ECSNode.ServerStatus.ACTIVE);
        }
        updateMetadata();
        //Then need to activate all the nodes
        return true;
    }

    @Override
    public boolean stop() throws Exception {
        List<IECSNode> listOfNodesToStop = new ArrayList<>();
        for (IECSNode node : nodePool) {
            if (node.getStatus().equals(ECSNode.ServerStatus.ACTIVE)) {
                listOfNodesToStop.add(node);
            }
        }

        // broadcast stop messages to all nodes to stop
        for (IECSNode node : listOfNodesToStop) {
            hashRing.removeNode(node.getNodeHash());
        }
        for (IECSNode node: listOfNodesToStop) {
            node.setStatus(ECSNode.ServerStatus.STOP);
        }
        updateMetadata();
        return true;
    }

    @Override
    public boolean shutdown() throws Exception {
        List<IECSNode> listOfNodesToShutdown = new ArrayList<>();
        for (Map.Entry<String, IECSNode> nodeEntry : initNodes.entrySet()) {
            IECSNode node = nodeEntry.getValue();
            listOfNodesToShutdown.add(node);
        }
        // broadcast shutdown message to all active nodes
        for (Map.Entry<String, IECSNode> nodeEntry : initNodes.entrySet()) {
            IECSNode node = nodeEntry.getValue();
            node.setStatus(ECSNode.ServerStatus.OFFLINE);
            nodePool.add(node);
        }
        initNodes.clear();
        hashRing.removeAllNodes();
        updateMetadata();
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

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        Collection<IECSNode> nodeToAdd = addNodes(1, cacheStrategy, cacheSize);
        if (nodeToAdd == null || nodeToAdd.size() != 1) { return null; }
        IECSNode node = (IECSNode) nodeToAdd.toArray()[0];
        return node;
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        Collection<IECSNode> listOfNodesToAdd = setupNodes(count, cacheStrategy, cacheSize);

        for (IECSNode node : listOfNodesToAdd) {
            String command = "java -jar " +
                    SERVER_JAR_PATH + " " +
                    node.getNodePort() + " " +
                    node.getNodeName() + " " +
                    ZK_IP + " " +
                    ZK_PORT;
            String sshCommand = "ssh -o StrictHostKeyChecking=no -n " + node.getNodeHost() + " nohup" + command + " &";

            try {
                Process proc = Runtime.getRuntime().exec(sshCommand);
                Thread.sleep(100);
            } catch (IOException e) {
                logger.error("IOException happened when launching nodes with SSH");
                listOfNodesToAdd.remove(node);
            } catch (InterruptedException e) {
                logger.error("Interrupted Exception happened when starting ssh command");
                listOfNodesToAdd.remove(node);
            }
        }

        try {
            boolean result = awaitNodes(count, ZK_TIMEOUT);
            return result ? listOfNodesToAdd: null;
        } catch(Exception e) {
            logger.error(e);
            return null;
        }
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        logger.info("Setting up " + count + " nodes with cache Strategy" + cacheStrategy);

        if (count <=0 || count > nodePool.size()) { return null; }
        List<IECSNode> listOfNodesToSetup = new ArrayList<>();
        for (int i = 0 ; i < count; i++ ) {
            IECSNode node = nodePool.poll();
            listOfNodesToSetup.add(node);
        }

        try {
            if (zk.exists(ZK_SERVER_ROOT, false) == null) {
                zk.create(ZK_SERVER_ROOT, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }

            ServerMetadata metadata = new ServerMetadata(cacheSize, cacheStrategy);
            byte[] metadataBytes = new Gson().toJson(metadata).getBytes();

            for (IECSNode node : listOfNodesToSetup) {
                String nodePath = getZKNodePath(node);
                Stat exists = zk.exists(nodePath, false);
                if (exists != null) {
                    zk.setData(nodePath, metadataBytes, exists.getVersion());
                    List<String> zNodeChildList = zk.getChildren(nodePath, false);
                    for (String  child : zNodeChildList) {
                        Stat childExists = zk.exists(nodePath + "/" + child, false);
                        zk.delete(nodePath + "/" + child, childExists.getVersion());
                    }
                } else { zk.create(nodePath, metadataBytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT); }
            }

        } catch (KeeperException | InterruptedException e) {
            logger.error("Exception occured during setUpNodes in zookeeper");
            return null;
        }

        for (IECSNode node : listOfNodesToSetup) {
            node.setStatus(ECSNode.ServerStatus.INACTIVE);
            initNodes.put(node.getNodeName(), node);
        }
        return listOfNodesToSetup;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        List<IECSNode> listOfNodesToWait = new ArrayList<>();

        for (Map.Entry<String, IECSNode> nodeEntry : initNodes.entrySet()) {
            IECSNode node = nodeEntry.getValue();
            if (node.getStatus().equals(ECSNode.ServerStatus.ACTIVE)) {
                listOfNodesToWait.add(node);
            }
        }

        for (IECSNode node : listOfNodesToWait) {
            node.setStatus(ECSNode.ServerStatus.STOP);
        }
        return true;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        List<IECSNode> listOfNodesToRemove = new ArrayList<>();

        for (Map.Entry<String, IECSNode> nodeEntry : initNodes.entrySet()) {
            IECSNode node = nodeEntry.getValue();
            if (nodeNames.contains(node.getNodeName())) {
                listOfNodesToRemove.add(node);
            }
        }
        // rearrange data
        return true;
    }



    @Override
    public Map<String, IECSNode> getNodes() { return initNodes; }

    @Override
    public IECSNode getNodeByKey(String key) { return hashRing.getNodeByKeyHash(key); }

    static String getZKNodePath(IECSNode node) { return ZK_SERVER_ROOT + "/" + node.getNodeName(); }
}
