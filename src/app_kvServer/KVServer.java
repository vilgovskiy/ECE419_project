package app_kvServer;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.zookeeper.*;
import com.google.gson.Gson;

import client.KVStore;
import ecs.*;
import ecs.ECS;
import logger.LogSetup;

import org.apache.zookeeper.data.Stat;
import server.KVTransfer;
import server.ReplicationManager;
import server.ServerMetadata;
import server.cache.*;
import server.sql.SQLExecutor;
import server.sql.SQLStorage;
import server.storage.*;
import server.KVClientConnection;
import shared.messages.JsonMessage;
import shared.messages.KVMessage;
import shared.messages.KVAdminMessage;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.SocketException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;


public class KVServer extends Thread implements IKVServer, Watcher {

    private static Logger logger = Logger.getRootLogger();
    private static final int maxKeyLength = 20;
    private static final int maxValueLength = 120 * 1024;

    public enum Status {
        START, /* Server running */
        STOP,  /* Server stopped */
        LOCK_WRITE /* Server locked for write */
    }

    private String name;
    private int port;
    private int cacheSize;
    private CacheStrategy strategy;

    private Status status;
    private boolean running;
    private ServerSocket serverSocket;

    /* Storage */
    private IKVStorage storage;
    private Cache cache;

    /* ECS Consistent hash ring */
	private ECSConsistentHash hashRingMetadata;
	private String start;
    private String end;

	/* Zookeeper */
	private ZooKeeper zk;
	private int zkPort;
	private String zkHost;
	private String zkPath;

	/* Replication */
    private ReplicationManager replicationManager;

    /* SQL */
    private SQLStorage sqlStorage;

    /**
     * Start KV Server at given port
     *
     * @param port      given port for storage server to operate
     * @param cacheSize specifies how many key-value pairs the server is allowed
     *                  to keep in-memory
     * @param strategy  specifies the cache replacement strategy in case the cache
     *                  is full and there is a GET- or PUT-request on a key that is
     *                  currently not contained in the cache. Options are "FIFO", "LRU",
     *                  and "LFU".
     */


    public KVServer(int port, int cacheSize, String strategy) {
        this.port = port;
        this.cacheSize = cacheSize;
        this.strategy = CacheStrategy.valueOf(strategy);
        this.status = Status.START;
        this.hashRingMetadata = new ECSConsistentHash();
        if (storage == null ) {
            storage = new KVStorage("storage");
        }
        if (sqlStorage == null) {
            sqlStorage = new SQLStorage("storage_db");
        }

        switch (this.strategy) {
            case FIFO:
                cache = new FIFOCache(cacheSize);
                break;
            case LRU:
                cache = new LRUCache(cacheSize);
                break;
            case LFU:
                cache = new LFUCache(cacheSize);
                break;
            default:
        }

        // Non-distributed case
        start = "00000000000000000000000000000000";
        end = "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF";

        logger.info("creating an instance of the KV server...");
    }

    public KVServer(int port, int cacheSize, String strategy, String name) {
        this.port = port;
        this.name = name;
        this.cacheSize = cacheSize;
        this.strategy = CacheStrategy.valueOf(strategy);
        this.status = Status.START;
		this.hashRingMetadata = new ECSConsistentHash();
        if (storage == null ) {
            storage = new KVStorage("storage");
        }
        if (sqlStorage == null) {
            sqlStorage = new SQLStorage("storage_db");
        }

        switch (this.strategy) {
			case FIFO:
				cache = new FIFOCache(cacheSize);
				break;
			case LRU:
				cache = new LRUCache(cacheSize);
				break;
			case LFU:
				cache = new LFUCache(cacheSize);
				break;
			default:
		}

		// Non-distributed case
        start = "00000000000000000000000000000000";
        end = "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF";

        logger.info("creating an instance of the KV server...");
    }

    public KVServer(int port, int cacheSize, String strategy,
					String name, String zkHost, int zkPort) throws IOException {
    	this(port, cacheSize, strategy, name);

		// on init, block all client requests
        this.status = Status.STOP;

		// ready to establish connection to Zookeeper host
        this.zkHost = zkHost;
		this.zkPort = zkPort;
		this.zkPath = ECS.ZK_SERVER_ROOT + "/" + name;
		String zkServer = zkHost + ":" + zkPort;

		// connect to zookeeper and retrieve previous cache info
		connectToZookeeper(zkServer);
        retrieveCacheInfo();

        // set watch for all other messages to this server
        try {
            List<String> children = zk.getChildren(zkPath, this, null);
            if (!children.isEmpty()) {
                String adminMsgPath = zkPath + "/" + children.get(0);
                KVAdminMessage adminMsg = new Gson().fromJson(
                        new String(zk.getData(adminMsgPath, false, null)), KVAdminMessage.class);
                if (adminMsg.getStatus().equals(KVAdminMessage.Status.READY)) {
                    Stat exists = zk.exists(adminMsgPath, false);
                    zk.delete(adminMsgPath, exists.getVersion());
                }
            }
        } catch (KeeperException | InterruptedException e){
            logger.error(" unable to retrieve zk children ");
            e.printStackTrace();
        }

        // set up failure detection
        setUpFailureDetection();
	}

    @Override
    public int getPort() {
        return this.port;
    }

    public String getServerName() {
        return this.name;
    }

    @Override
    public String getHostname() {
        return "127.0.0.1";
    }

    @Override
    public CacheStrategy getCacheStrategy() {
        return strategy;
    }

    @Override
    public int getCacheSize() {
        return cacheSize;
    }

    @Override
    public boolean inStorage(String key) {
        try {
            return storage.checkIfKeyExists(key);
        } catch (Exception e) {
            logger.error("Error while searching key: " + key + " in storage", e);
            return false;
        }
    }

    @Override
    public boolean inCache(String key) {
        return cache.inCache(key);
    }

    @Override
    public JsonMessage getKV(String key)  {
        JsonMessage responseMsg = new JsonMessage();
        responseMsg.setKey(key);

		if (cache.inCache(key)) {
            responseMsg.setStatus(KVMessage.StatusType.GET_SUCCESS);
		    responseMsg.setValue(cache.getKV(key));
		} else {
		    if (inStorage(key)) {
                try {
                    KVData foundEntry = storage.read(key);
                    String value = foundEntry.getValue();
                    if (value.equals("")) {
                        responseMsg.setValue("");
                        responseMsg.setStatus(KVMessage.StatusType.GET_ERROR);
                        cache.putKV(key, value);
                        logger.info("GET_ERROR for <key: " + key + ", value: " + foundEntry.getValue() + ">");
                    }
                    else {
                        responseMsg.setValue(value);
                        responseMsg.setStatus(KVMessage.StatusType.GET_SUCCESS);
                        cache.putKV(key, value);
                        logger.info("GET_SUCCESS for <key: " + key + ", value: " + foundEntry.getValue() + ">");
                    }
                } catch (Exception e) {
                    logger.error("Error while finding key: " + key + " from storage", e);
                    responseMsg.setStatus(KVMessage.StatusType.GET_ERROR);
                }
            } else {
		        responseMsg.setStatus(KVMessage.StatusType.GET_ERROR);
            }
		}
        return responseMsg;
    }

    @Override
    public JsonMessage putKV(String key, String value) {
        boolean entryExists = inStorage(key);
        JsonMessage responseMsg = new JsonMessage();
        responseMsg.setKey(key);
        responseMsg.setValue(value);

        if (value.isEmpty()) {
            logger.info("Server " + name + " deleting entry with key: " + key);

			if (cache.inCache(key)) {
				cache.putKV(key, null);
			}
            if (entryExists) {
                try {
                    storage.write(new KVData(key, ""));
                    logger.info("Server " + name + " successfully deleted entry with key: " + key);
                    responseMsg.setStatus(KVMessage.StatusType.DELETE_SUCCESS);
                } catch (Exception e) {
                    responseMsg.setStatus(KVMessage.StatusType.DELETE_ERROR);
                    logger.warn("Server " + name +
                            " error while deleting key: " + key + " from storage", e);
                }
            } else {
                responseMsg.setStatus(KVMessage.StatusType.DELETE_ERROR);
                logger.warn("Server " + name + " unable to delete key: "
                        + key + ", the key doesn't exist!");
            }
        } else {
			responseMsg.setStatus(KVMessage.StatusType.PUT_ERROR);
			if (checkValidKeyValue(key, value)){
            	try {
            	    storage.write(new KVData(key, value));
					cache.putKV(key, value);

					if (entryExists) {
                        responseMsg.setStatus(KVMessage.StatusType.PUT_UPDATE);
                        logger.info("Server " + name + " successfully updated <key: "
                                + key + ", value: " + value + ">");
                    } else {
                        responseMsg.setStatus(KVMessage.StatusType.PUT_SUCCESS);
                        logger.info("Server " + name + " successfully inserted <key: "
                                + key + ", value: " + value + ">");
                    }
            	} catch (Exception e) {
            	    logger.error("Server " + name + " unable to put <key: "
                            + key + ", value: " + value + "> in storage", e);
            	    responseMsg.setStatus(KVMessage.StatusType.PUT_ERROR);
                }
			}
        }
        return responseMsg;
    }


    @Override
    public JsonMessage sql(String sql) {
        JsonMessage responseMsg = new JsonMessage();
        responseMsg.setKey(sql);

        String[] sqlStrings = sql.split("\\s+");
        String actionType = sqlStrings[0];
        SQLExecutor executor = new SQLExecutor(sqlStorage);

        switch(actionType) {
            case "select":
            case "SELECT":
                List<String> queryResult = executor.query(sql);
                if (queryResult != null) {
                    responseMsg.setValue(String.join("\n", queryResult));
                    responseMsg.setStatus(KVMessage.StatusType.SQL_SUCCESS);
                } else {
                    responseMsg.setStatus(KVMessage.StatusType.SQL_ERROR);
                }
                break;

            case "create":
            case "CREATE":
                boolean result = executor.create(sql);
                if (result) {
                    responseMsg.setStatus(KVMessage.StatusType.SQL_SUCCESS);
                } else {
                    responseMsg.setStatus(KVMessage.StatusType.SQL_ERROR);
                }
                break;

            case "insert":
            case "INSERT":
                result = executor.insert(sql);
                if (result) {
                    responseMsg.setStatus(KVMessage.StatusType.SQL_SUCCESS);
                } else {
                    responseMsg.setStatus(KVMessage.StatusType.SQL_ERROR);
                }
                break;

            case "delete":
            case "DELETE":
                result = executor.delete(sql);
                if (result) {
                    responseMsg.setStatus(KVMessage.StatusType.SQL_SUCCESS);
                } else {
                    responseMsg.setStatus(KVMessage.StatusType.SQL_ERROR);
                }
                break;

            case "drop":
            case "DROP":
                result = executor.drop(sql);
                if (result) {
                    responseMsg.setStatus(KVMessage.StatusType.SQL_SUCCESS);
                } else {
                    responseMsg.setStatus(KVMessage.StatusType.SQL_ERROR);
                }
                break;

            case "update":
            case "UPDATE":
                result = executor.create(sql);
                if (result) {
                    responseMsg.setStatus(KVMessage.StatusType.SQL_SUCCESS);
                } else {
                    responseMsg.setStatus(KVMessage.StatusType.SQL_ERROR);
                }
                break;
        }
        return responseMsg;
    }

    @Override
    public void clearCache() {
        cache.clearCache();
    }

    @Override
    public void clearStorage() {
        clearCache();
        storage.clearStorage();
    }

    @Override
    public void run() {
        running = initializeKVServer();

        if (serverSocket != null) {
            while (isRunning()) {
                try {
                    Socket client = serverSocket.accept();
                    KVClientConnection connection = new KVClientConnection(this, client);
                    new Thread(connection).start();

                    logger.info("Server " + name + " connected to client "
                            + client.getInetAddress().getHostName()
                            + " on port " + client.getPort());
                } catch (SocketException se) {
					logger.error("Server " + name + " Error! Socket exception during connection. ");
				} catch (IOException e) {
                    logger.error("Server " + name + " Error! " +
                            "Unable to establish connection. \n", e);
                } catch (Exception e) {
                    logger.error("Server " + name + " Error! Problem occured during connection");
                }
            }
            logger.info("Server " + name + " is stopped");
        }
    }

    @Override
    public void kill() {
        this.running = false;
        try {
            if (serverSocket != null) serverSocket.close();
            if (this.replicationManager != null) {
                this.replicationManager.clearReplicatorList();
            }
        } catch (IOException e) {
            logger.error("Server " + name + " failed to close socket!");
        }
    }

    @Override
    public void close() {
        kill();
        clearCache();
    }

    private boolean initializeKVServer() {

        logger.info("Server " + name + " Starting KVServer...");
        try {
            serverSocket = new ServerSocket(port);
            this.port = serverSocket.getLocalPort();
            logger.info("Server " + name + " listening on "+ getHostname() + ":" + serverSocket.getLocalPort());

            // set watch for metadata
            setUpHashRingMetadata();
            this.replicationManager = new ReplicationManager(name, getHostname(), this.port);
            return true;
        } catch (IOException e) {
            logger.error("Server " + name + " Cannot open server socket!");
            if (e instanceof BindException) {
                logger.error("Port " + port + " is already bound!");
            }
            return false;
        }
    }

	@Override
	public ECSConsistentHash getHashRingMetadata() {
        return hashRingMetadata;
	}

    public ReplicationManager getReplicationManager(){
        return replicationManager;
    }

    // check if KVserver is responsible for the key's hash
	@Override
	public synchronized boolean inServerKeyRange(String key, String start, String end) {
    	if (start.compareTo(end) < 0) {
			return key.compareTo(start) >= 0 && key.compareTo(end) < 0;
		} else {
    		// case where the range wraps around the ring
			return key.compareTo(start) >= 0 || key.compareTo(end) < 0;
		}
	}

	@Override
	public boolean serverStopped() {
        return status.equals(Status.STOP);
	}

	@Override
	public boolean writeLocked() {
        return status.equals(Status.LOCK_WRITE);
	}

	public void updateHashRingMetadata(String jsonRing) {
		// update metadata
		this.hashRingMetadata.updateConsistentHash(jsonRing);

		// update the range this server is responsible for
		String key = getHostname() + ":" + getPort();
		String hashedKey = ECSNode.calculateHash(key);
		IECSNode n = this.hashRingMetadata.getNodeByKeyHash(hashedKey);
		if (n != null) {
            this.setRange(n.getNodeHashRange());
        }
	}

	private synchronized void setRange(String[] range) {
        if (range != null) {
            this.start = range[0];
            this.end = range[1];
        }
	}

	public synchronized String[] getRange() {
    	return new String[] {start, end};
	}

	public void lockWrite() {
		logger.info("Server " + name + " write operations have been locked");
		this.status = Status.LOCK_WRITE;
	}

	public void unlockWrite() {
		logger.info("Server " + name + " write operations have been unlocked");
	    this.status = Status.START;
    }

	public void rejectClientRequests() {
		logger.info("Server " + name + " is now blocking client requests");
		this.status = Status.STOP;
	}

	public void acceptClientRequests() {
		logger.info("Server " + name + " is now processing client requests");
		this.status = Status.START;
	}

	private void moveData(String start, String end, String address, int port) {
		Map<String, String> allKVData = new HashMap<>();

		try {
			allKVData = storage.getAllKVData();
		} catch (Exception e) {
			logger.error("Server " + name + " could not get all KV Pairs from storage");
		}

		KVTransfer transfer = new KVTransfer(address, port);

		// try to connect to the server
		try {
			transfer.connect();
		} catch (Exception e) {
			logger.error("Server " + name + " could not connect to " + address + ":" + port + "to transfer data");
		}

		for (Map.Entry<String, String> entry : allKVData.entrySet()) {
			String key = entry.getKey();
			String value = entry.getValue();
			String hashedKey = ECSNode.calculateHash(key);

			if (inServerKeyRange(hashedKey, start, end)) {
				// send data to the server
				try {
					transfer.put(key, value);
				} catch (Exception e) {
					logger.error("Server " + name + " could not send <" + key + "," + value + "> to " + address + ":" + port);
				}
			}
		}
		transfer.close();
	}

	private boolean checkValidKeyValue(String key, String value) {
		if (key == null || key.equals("") || key.contains(" ") || key.getBytes().length > maxKeyLength
				|| value.getBytes().length > maxValueLength) return false;
		else return true;
	}

	private boolean isRunning() {
		return this.running;
	}

	private void setUpFailureDetection() {
        try {
            if (zk.exists(ECS.ZK_ALIVE_PATH, false) != null) {
                String path = ECS.ZK_ALIVE_PATH + "/" + this.name;
                ServerMetadata serverMetadata = new ServerMetadata(cacheSize, strategy.toString());
                String jsonMetadata = new Gson().toJson(serverMetadata);
                zk.create(path, jsonMetadata.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            } else {
                logger.error("Unable to setup failure detection node - zookeeper alive path does not exist");
            }
        } catch (KeeperException | InterruptedException e) {
            logger.error("Unable to set up failure detection node");
        }
    }

	private void setUpHashRingMetadata() {
        try {
            byte[] hashRing = zk.getData(ECS.ZK_METADATA_PATH, new Watcher() {
                // update metadata
                public void process(WatchedEvent we) {
                    try {
                        byte[] hashRing = zk.getData(ECS.ZK_METADATA_PATH, this, null);
                        String jsonRing = new String(hashRing);
                        updateHashRingMetadata(jsonRing);
                        if (replicationManager != null ) {
                            replicationManager.updateReplicatorList(hashRingMetadata);
                        }
                    } catch (KeeperException | InterruptedException e) {
                        logger.error("Server " + name + " unable to update the metadata node");
                        e.printStackTrace();
                    } catch (IOException ioe) {
                        logger.error("Server " + name + " unable to update metadata for replication manager");
                        ioe.printStackTrace();
                    }
                }
            },null);

            String jsonRing = new String(hashRing);
            updateHashRingMetadata(jsonRing);
        } catch (InterruptedException | KeeperException e) {
            logger.debug("Server " + name + " unable to get metadata info from zookeeper");
            e.printStackTrace();
        }
    }

	private void retrieveCacheInfo() {
        logger.debug("Server " + name + " retrieving cache info from zookeeper " + zkHost +":"+ zkPort);
        try {
            if (zk.exists(zkPath, false) != null) {
                byte[] cacheBytes= zk.getData(zkPath, false, null);
                String cache = new String(cacheBytes);

                ServerMetadata jsonMetadata = new Gson().fromJson(cache, ServerMetadata.class);
                this.cacheSize = jsonMetadata.getCacheSize();
                this.strategy = CacheStrategy.valueOf(jsonMetadata.getCacheStrategy());
                logger.debug("Server " + name + " cache info cacheSize: " + cacheSize + " strategy:" + strategy +
                        " retrieved from ZK " + zkPath);
            } else logger.error("Server " + name + " doesn't exist at ZK path" + zkPath);
        } catch (InterruptedException | KeeperException e) {
            logger.error("Server " + name + " cannot retrieve cache from path " + zkPath);
            this.cacheSize = 50;
            this.strategy = CacheStrategy.FIFO;
        }
    }

	private void connectToZookeeper(String zkServer) throws IOException {
        // use countdown latch to only start after zookeeper is connected
        final CountDownLatch zkLatch = new CountDownLatch(0);
		zk = new ZooKeeper(zkServer, ECS.ZK_TIMEOUT, new Watcher() {
			public void process(WatchedEvent event) {
				if (event.getState().equals(Event.KeeperState.SyncConnected)) {
                    zkLatch.countDown();
				}
			}
		});

		try {
			// wait until we are connected
            zkLatch.await();
            logger.debug("Server " + name + " connected to zookeeper at "+ zkServer);
		} catch (InterruptedException e) {
			logger.error("Server " + name + " Error encountered while connecting to zookeeper: "
						+ e.getMessage());
		}
	}

	/* Process all messages from ECS to KVServer using zookeeper */
	@Override
	public void process(WatchedEvent event) {
		List<String> children;
		try {
			children = zk.getChildren(zkPath, false, null);
			if (children.isEmpty()) {
				// no message to process, just re-register watch
				zk.getChildren(zkPath, this, null);
				return;
			}

			String path = zkPath + "/" + children.get(0);
			if (children.size() > 1) {
				String errorMsg = ("Server " + name + " expects only one message, received "
									+ children.size());
				logger.error(errorMsg);
				zk.setData(path, errorMsg.getBytes(), zk.exists(path, false).getVersion());
			}

			byte[] data = zk.getData(path, false, null);
			KVAdminMessage adminMessage = new KVAdminMessage();
			String json = new String(data);
            adminMessage.deserialize(json);
            Stat exists;

			switch (adminMessage.getStatus()) {
                case READY:
                    exists = zk.exists(path, false);
                    zk.delete(path, exists.getVersion());
                    break;

                case START:
                    acceptClientRequests();
                    exists = zk.exists(path, false);
                    zk.delete(path, exists.getVersion());
                    break;

                case STOP:
                    rejectClientRequests();
                    exists = zk.exists(path, false);
                    zk.delete(path, exists.getVersion());
                    break;

                case SHUT_DOWN:
                    rejectClientRequests();
                    exists = zk.exists(path, false);
                    zk.delete(path, exists.getVersion());
                    close();
                    break;

                case LOCK_WRITE:
                    lockWrite();
                    exists = zk.exists(path, false);
                    zk.delete(path, exists.getVersion());
                    break;

                case UNLOCK_WRITE:
                    unlockWrite();
                    exists = zk.exists(path, false);
                    zk.delete(path, exists.getVersion());
                    break;

                case MOVE_DATA:
                    ArrayList<String> args = adminMessage.getArgs();

                    if (args.size() != 4) {
                        String errorMsg = ("Incorrect number of args "
                                + "sent to MOVE_DATA command");
                        exists = zk.exists(path, false);
                        zk.setData(path, errorMsg.getBytes(), exists.getVersion());
                    } else {
                        moveData(args.get(0), args.get(1), args.get(2),
                                Integer.parseInt(args.get(3)));
                        exists = zk.exists(path, false);
                        zk.delete(path, exists.getVersion());
                    }
                    break;
            }
				// re-register watch
				if (isRunning()) {
					zk.getChildren(zkPath, this, null);
				}
			} catch (KeeperException | InterruptedException e) {
            logger.error("Server " + name + " unable to process the watcher event");
        }
	}

	/**
     * Helper function that checks all argument types and if they are vcalid
     *
     * @param port integer signifies port server is listening on
     * @param cache_size integer size of server cache
     * @param cache_strategy one of three implemented caching strategies [FIFO, LIFO, LFU]
     * @return true or false based of weather args passed validation
     */
    private static boolean argsAreOkay(String port, String cache_size, String cache_strategy){
        boolean ret;
        if (port.matches("\\d+")) {
            if (cache_size.matches("\\d+")) {
                ret = true;
            } else {
                ret = false;
                System.out.println("Error! Invalid argument <cachesize>! Not a number!");
            }
        } else {
            ret = false;
            System.out.println("Error! Invalid argument <port>! Not a number!");
        }

        if (!(cache_strategy.matches("LFU") ||
                cache_strategy.matches("FIFO") ||
                cache_strategy.matches("LIFO"))) {
            ret = false;
            System.out.println("Error! Invalid argument <cache_strategy>! Has to be either \"FIFO\" or \"LIFO\" or \"LFU\"!");
        }
        return ret;
    }


    /**
     * Main entry point for the echo server application.
     *
     * @param args contains the port number at args[0], cache size at args[1] and caching strategy at args[2].
     */
    public static void main(String[] args) {
        try {
			new LogSetup("logs/server.log", Level.INFO);

            if (args.length != 3 && args.length != 6) {
                System.out.println("Error! Invalid number of arguments!");
                System.out.println("Usage: KVServer <port> <cache size> <strategy>!");
				System.out.println("Usage: KVServer <port> <cache size> <strategy> <name> <zkHost> <zkPort>!");
            } else if (args.length == 3 && argsAreOkay(args[0], args[1], args[2])) {
                int port = Integer.parseInt(args[0]);
                int cacheSize = Integer.parseInt(args[1]);
                String strategy = args[2];

                new KVServer(port, cacheSize, strategy, "storage").start();
            } else {
				int port = Integer.parseInt(args[0]);
				int cacheSize = Integer.parseInt(args[1]);
				String strategy = args[2];
				String name = args[3];
				String zkHost = args[4];
				int zkPort = Integer.parseInt(args[5]);

				new KVServer(port, cacheSize, strategy, name, zkHost, zkPort).start();
			}
        } catch (NumberFormatException nfe) {
            System.out.println("Error! Invalid argument <port> or <cache_size>! Not a number!");
            System.out.println("Usage: Server <port>!");
            System.exit(1);
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
		}
    }
}
