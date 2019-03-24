package ecs;


import app_kvECS.IECSClient;
import server.ServerMetadata;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import com.google.gson.Gson;
import shared.messages.KVAdminMessage;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;


public class ECS implements IECSClient {

    private static Logger logger = Logger.getRootLogger();

    public static final String ZK_IP = "127.0.0.1";
    public static final String ZK_PORT = "49999";
    //public static final Integer ZK_TIMEOUT = 5000;
    public static final Integer ZK_TIMEOUT = 20000;

    public static final String ZK_METADATA_PATH = "/metadata";
    public static final String ZK_SERVER_ROOT = "/kv_servers";
    public static final String SERVER_JAR_PATH = new File(System.getProperty("user.dir"),
            "m2-server.jar").toString();



    // Set of all servers available in the system in config file
    private Queue<IECSNode> nodePool = new ConcurrentLinkedQueue<>();

    //Holds all currently initialized nodes
    private Map<String, IECSNode> initNodes = new HashMap<>();

    //Contains all all hash mappings of all currently ACTIVE servers
    private ECSConsistentHash hashRing = new ECSConsistentHash();
    private ZooKeeper zk;

    public ECS(String configFile) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(new File(configFile)));
        String cur;

        while((cur = br.readLine()) != null) {
            String[] tokens = cur.split(" ");
            ECSNode node = new ECSNode(tokens[0], tokens[1], Integer.parseInt(tokens[2]));
            nodePool.add(node);
            logger.debug("Server " + tokens[0] + " at " + tokens[1] + ":" + tokens[2] + " added to nodePool");
        }

        final CountDownLatch latch = new CountDownLatch(0);
        zk = new ZooKeeper(ZK_IP + ":" + ZK_PORT, ZK_TIMEOUT, new Watcher() {
            public void process(WatchedEvent event) {
                if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                    latch.countDown();
                }
            }
        });
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Interrupted Exception while creating ECS");
            e.printStackTrace();
        }
        updateMetadata();
        logger.info("ECS initialized at zookeeper " + ZK_IP + ":" + ZK_PORT);
    }

    @Override
    public String getHashRingInfo() {
        return this.hashRing.serializeHashRingPretty();
    }

    @Override
    public String getAllNodesInfo() {
        Map<String, ECSNode.ServerStatus> allNodesInfo = new HashMap<>();

        for (IECSNode node : nodePool) {
            allNodesInfo.put(node.getNodeName(), node.getStatus());
        }

        for (IECSNode node : initNodes.values()) {
            allNodesInfo.put(node.getNodeName(), node.getStatus());
        }
        return allNodesInfo.toString();
    }

    @Override
    public boolean start() throws Exception {
        //Assume that servers are already initialized and currently in STOPPED state
        List<IECSNode> listOfNodesToStart = new ArrayList<>();
        for(IECSNode node : initNodes.values()){
            if (node.getStatus().equals(ECSNode.ServerStatus.STOP)){
                listOfNodesToStart.add(node);
            }
        }
        // add all the initialized nodes and currently in STOPPED state to hash ring
        for(IECSNode node : listOfNodesToStart){
            hashRing.addNode(node);
        }
        redistributeData(listOfNodesToStart);

        // Need to broadcast KVadmin message of START to all nodes that are starting
        ECSCommunication broadcaster = new ECSCommunication(zk, listOfNodesToStart);
        KVAdminMessage adminMsg = new KVAdminMessage(KVAdminMessage.Status.START);
        broadcaster.broadcast(adminMsg);

        // Set all status of starting nodes to ACTIVE
        for (IECSNode node : listOfNodesToStart) {
            node.setStatus(ECSNode.ServerStatus.ACTIVE);
        }

        updateMetadata();
        return true;
    }

    @Override
    public boolean stop() throws Exception {

        List<IECSNode> listOfNodesToStop = new ArrayList<>();
        // add all nodes that are currently ACTIVE to nodes that needs to be stopped
        for (IECSNode node : nodePool) {
            if (node.getStatus().equals(ECSNode.ServerStatus.ACTIVE)) {
                listOfNodesToStop.add(node);
            }
        }

        // broadcast stop messages to all the ACTIVE nodes to stop
        ECSCommunication broadcaster = new ECSCommunication(zk, listOfNodesToStop);
        KVAdminMessage adminMsg = new KVAdminMessage(KVAdminMessage.Status.STOP);
        broadcaster.broadcast(adminMsg);

        // Remove all nodes that are stopping from hash ring
        for (IECSNode node : listOfNodesToStop) {
            hashRing.removeNode(node.getNodeHash());
        }

        // Set all status of all nodes that were stopped to STOP
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
            if (node.getStatus().equals(ECSNode.ServerStatus.ACTIVE)) {
                listOfNodesToShutdown.add(node);
            }
        }

        // broadcast shutdown message to active nodes
        ECSCommunication broadcaster = new ECSCommunication(zk, listOfNodesToShutdown);
        KVAdminMessage adminMsg = new KVAdminMessage(KVAdminMessage.Status.SHUT_DOWN);
        broadcaster.broadcast(adminMsg);

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
     * Push the metadata(hash ring info) to ZooKeeper z-node
     */
    private boolean updateMetadata() {
        try {
            Stat exists = zk.exists(ZK_METADATA_PATH, false);
            if (exists == null) {
                logger.debug("Updating hash ring info " + hashRing.serializeHashRingPretty());
                zk.create(ZK_METADATA_PATH, hashRing.serializeHashRing().getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } else {
                logger.debug("Updating hash ring info " + hashRing.serializeHashRingPretty());
                zk.setData(ZK_METADATA_PATH, hashRing.serializeHashRing().getBytes(),
                        exists.getVersion());
            }
            return true;
        } catch (InterruptedException e) {
            logger.error("Interrupted Exception happened when updating metadata in ECS");
            return false;
        } catch (KeeperException e) {
            logger.error("KeeperException happened when updating metadata in ECS");
            return false;
        }
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
            String command = "java -jar" + " " +
                    SERVER_JAR_PATH + " " +
                    String.valueOf(node.getNodePort()) + " " +
                    cacheSize + " " +
                    cacheStrategy + " " +
                    node.getNodeName() + " " +
                    ZK_IP + " " +
                    ZK_PORT;
            String sshCommand = "ssh -o StrictHostKeyChecking=no -n " + node.getNodeHost() + " nohup " + command +
                    " > " + getServerLogPath(node.getNodeName());
            logger.info("Adding node " + node.getNodeName()  + ", ssh command " + sshCommand);
            try {
                Process proc = Runtime.getRuntime().exec(sshCommand);
                Thread.sleep(100);
            } catch (IOException e) {
                logger.error("IOException happened when adding node " + node.getNodeName() +
                        " executing SSH command " + sshCommand);
                listOfNodesToAdd.remove(node);
            } catch (InterruptedException e) {
                logger.error("Interrupted Exception happened when adding node " + node.getNodeName() +
                        " executing SSH command " + sshCommand);
                listOfNodesToAdd.remove(node);
            }
        }

        try {
            boolean result = awaitNodes(count, ZK_TIMEOUT);
            return result ? listOfNodesToAdd: null;
        } catch(Exception e) {
            e.printStackTrace();
            return null;
        }
    }


    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        logger.info("Setting up " + count + " nodes with cache Strategy" + cacheStrategy);

        if (count <=0 || count > nodePool.size()) return null;
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
                    logger.debug("Setting node " + node.getNodeName() + ", zkPath: " + nodePath +
                            ", metadata: " + metadata.toString());
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
            if (node.getStatus().equals(ECSNode.ServerStatus.INACTIVE)) {
                listOfNodesToWait.add(node);
            }
        }

        ECSCommunication broadcaster = new ECSCommunication(zk, listOfNodesToWait);
        KVAdminMessage adminMsg = new KVAdminMessage(KVAdminMessage.Status.READY);
        broadcaster.broadcast(adminMsg);

        for (IECSNode node : listOfNodesToWait) {
            node.setStatus(ECSNode.ServerStatus.STOP);
        }
        return true;
    }

    // Remove nodes by node names
    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        List<IECSNode> listOfNodesToRemove = new ArrayList<>();

        // if the node's name is in the nodeNames collection, add to listOfNodesToRemove
        for (Map.Entry<String, IECSNode> nodeEntry : initNodes.entrySet()) {
            IECSNode node = nodeEntry.getValue();
            if (nodeNames.contains(node.getNodeName())) {
                listOfNodesToRemove.add(node);
            }
        }

        // if the nodes that are getting removed are currently ACTIVE, needs to redistribute
        // the data they have
        List<IECSNode> listOfNodesToRedistribute = new ArrayList<>();
        for (IECSNode node : listOfNodesToRemove) {
            if (node.getStatus().equals(ECSNode.ServerStatus.ACTIVE)) {
                listOfNodesToRedistribute.add(node);
            }
        }
        redistributeData(listOfNodesToRedistribute);

        // broadcast SHUTDOWN message to the listOfNodesToRemove
        ECSCommunication broadcaster = new ECSCommunication(zk, listOfNodesToRemove);
        KVAdminMessage adminMsg = new KVAdminMessage(KVAdminMessage.Status.SHUT_DOWN);
        broadcaster.broadcast(adminMsg);

        // TODO: might be wrong
        for (IECSNode node : listOfNodesToRemove) {
            if (node.getStatus().equals(ECSNode.ServerStatus.ACTIVE)) {
                node.setStatus(ECSNode.ServerStatus.OFFLINE);
                hashRing.removeNode(node.getNodeName());
                initNodes.remove(node.getNodeName());
                nodePool.add(node);
            }
        }
        updateMetadata();
        return true;
    }

    @Override
    public Map<String, IECSNode> getNodes() { return initNodes; }

    @Override
    public IECSNode getNodeByKey(String key) { return hashRing.getNodeByKeyHash(key); }

    static String getZKNodePath(IECSNode node) { return ZK_SERVER_ROOT + "/" + node.getNodeName(); }


    public boolean redistributeData(Collection<IECSNode> targetNodes) {
        Set<IECSNode> nodesToTransferFrom = new HashSet<>();
        Set<IECSNode> nodesToTransferTo = new HashSet<>();

        for (IECSNode node : targetNodes) {
            if (node.getStatus().equals(ECSNode.ServerStatus.ACTIVE)) {
                nodesToTransferFrom.add(node);
            } else if (node.getStatus().equals(ECSNode.ServerStatus.STOP)) {
                nodesToTransferTo.add(node);
            }
        }

        for (IECSNode node : targetNodes) {
            if (node.getStatus().equals(ECSNode.ServerStatus.ACTIVE)) {
                IECSNode nodeToTransferTo = getNextAvailableNode(node, nodesToTransferFrom);
                if (nodeToTransferTo != null) {
                    transferData(node, nodeToTransferTo, node.getNodeHashRange());
                } else {
                    logger.warn("No available node to transfer data to...");
                }
            } else if (node.getStatus().equals(ECSNode.ServerStatus.STOP)) {
                IECSNode nodeToTransferFrom = getNextAvailableNode(node, nodesToTransferTo);
                if (nodeToTransferFrom != null) {
                    transferData(nodeToTransferFrom, node, node.getNodeHashRange());
                } else {
                    logger.warn("No available node to transfer data from to node" + node.getNodeName());
                }
            } else {
                logger.error("Node data redistribution failed");
            }
        }
        return true;

    }


    public IECSNode getNextAvailableNode(IECSNode node, Set<IECSNode> conditionNodeSet) {
        IECSNode foundNode = node;
        int counter = 0;

        while(true) {
            foundNode = hashRing.findNextNode(foundNode.getNodeHash());
            if (!conditionNodeSet.contains(node)) {
                return foundNode;
            }
            if (foundNode.equals(node)) {
                return null;
            }
            counter += 1;
            if (2 * hashRing.getRingSize() < counter ) { return null; }
        }
    }


    private boolean transferData(IECSNode src, IECSNode dst, String[] nodeHashRange) {
        String dstHost = dst.getNodeHost();
        String dstPort = String.valueOf(dst.getNodePort());
        Collection<IECSNode> toTransfer = new ArrayList<>();

        toTransfer.add(src);
        ArrayList<String> args = new ArrayList<>();
        args.add(nodeHashRange[0]);
        args.add(nodeHashRange[1]);
        args.add(dstHost);
        args.add(dstPort);

        // broadcast the lock write message
        ECSCommunication writeLock = new ECSCommunication(zk, toTransfer);
        KVAdminMessage adminMsg1 = new KVAdminMessage(KVAdminMessage.Status.LOCK_WRITE);
        writeLock.broadcast(adminMsg1);

        // broadcast the move data message
        ECSCommunication moveData = new ECSCommunication(zk, toTransfer);
        KVAdminMessage adminMsg2 = new KVAdminMessage(KVAdminMessage.Status.MOVE_DATA, args);
        moveData.broadcast(adminMsg2);

        // broadcast the unlock write message
        ECSCommunication writeUnlock = new ECSCommunication(zk, toTransfer);
        KVAdminMessage adminMsg3 = new KVAdminMessage(KVAdminMessage.Status.UNLOCK_WRITE);
        writeUnlock.broadcast(adminMsg3);

        return true;
    }

    private String getServerLogPath(String nodeName) {
        String logPath = "logs/server_" + nodeName + ".log";
        return new File(System.getProperty("user.dir"), logPath).toString();
    }
}
