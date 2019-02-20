package app_kvServer;

import client.KVStore;
import ecs.*;
import ecs.ECS;
import logger.LogSetup;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.zookeeper.*;
import org.apache.zookeeper.Zookeeper;
import org.apache.zookeeper.data.Stat;
import server.cache.*;
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

    private int port, cacheSize, zkPort;
    private CacheStrategy strategy;
    private boolean running, stopped, lock_writes;
    private ServerSocket serverSocket;
    private IKVStorage storage;
	private Cache cache;
	private ECSConsistentHash metadata;
	private String start, end;
	private Map<String, String> toDelete;
	private String zkHost, name, zkPath;


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

    public KVServer(int port, int cacheSize, String strategy, String zkHost,
					int zkPort, String name) throws IOException {
        this.port = port;
        this.cacheSize = cacheSize;
        this.strategy = CacheStrategy.valueOf(strategy);
        if (storage == null ) storage = KVStorage.getInstance();

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

		this.toDelete = new HashMap<String, String>();
        logger.info("creating an instance of the KV server...");

		// establish connection to Zookeeper host
		this.zkHost = zkHost;
		this.zkPort = zkPort;
		this.name = name;
		this.zkPath = ECS.ZK_SERVER_ROOT + "/" + name;
		String zkServer = zkHost + ":" + Integer.toString(zkPort);
		connectToZookeeper(zkServer);

		// set watch for metadata
		try {
			byte[] hashRing = zk.getData(ECS.ZK_METADATA_ROOT, new Watcher() {
				// update metadata
				public void process(WatchedEvent we) {
					try {
						byte[] hashRing = zk.getData(ECS.ZK_METADATA_ROOT, this, null);
						String hashRingString = new String(hashRing);
						metadata = new ECSConsistentHash(hashRingString);
					} catch (KeeperException | InterruptedException e) {
						logger.error("Unable to update the metadata node");
						e.printStackTrace();
					}
				}
			}, null);

			String hashRingString = new String(hashRingData);
			this.metadata = new ECSConsistentHash(hashRingString);

		} catch (InterruptedException | KeeperException e) {
			logger.debug("Unable to get metadata info");
			e.printStackTrace();
		}

		// set watch for all other messages to this server
		zk.getChildren(zkPath, this, null);
    }

    @Override
    public int getPort() {
        return serverSocket.getLocalPort();
    }

    @Override
    public String getHostname() {
        if (serverSocket != null) {
            return serverSocket.getInetAddress().getHostName();
        }
        return null;
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
            logger.info("Deleting entry with key: " + key);

			if (cache.inCache(key)) {
				cache.putKV(key, null);
			}
            if (entryExists) {
                try {
                    storage.write(new KVData(key, ""));
                    logger.info("Successfully deleted entry with key: " + key);
                    responseMsg.setStatus(KVMessage.StatusType.DELETE_SUCCESS);
                } catch (Exception e) {
                    responseMsg.setStatus(KVMessage.StatusType.DELETE_ERROR);
                    logger.warn("Error while deleting key: " + key + " from storage", e);
                }
            } else {
                responseMsg.setStatus(KVMessage.StatusType.DELETE_ERROR);
                logger.warn("Unable to delete key: " + key + ", the key doesn't exist!");
            }
        } else {
			responseMsg.setStatus(KVMessage.StatusType.PUT_ERROR);
			if (checkValidKeyValue(key, value)){
            	try {
            	    storage.write(new KVData(key, value));
					cache.putKV(key, value);

					if (entryExists) {
                        responseMsg.setStatus(KVMessage.StatusType.PUT_UPDATE);
                        logger.info("Successfully updated <key: " + key + ", value: " + value + ">");
                    } else {
                        responseMsg.setStatus(KVMessage.StatusType.PUT_SUCCESS);
                        logger.info("Successfully inserted <key: " + key + ", value: " + value + ">");
                    }
            	} catch (Exception e) {
            	    logger.error("Unable to put <key: " + key + ", value: " + value + "> in storage", e);
            	    responseMsg.setStatus(KVMessage.StatusType.PUT_ERROR);
                }
			}
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

                    logger.info("Connected to "
                            + client.getInetAddress().getHostName()
                            + " on port " + client.getPort());
                } catch (SocketException se) {
					logger.error("Error! Socket exception during connection. ");
				} catch (IOException e) {
                    logger.error("Error! " +
                            "Unable to establish connection. \n", e);
                } catch (Exception e) {
                    logger.error("Error! Problem occured during connection");
                }
            }
            logger.info("Server is stopped");
        }
    }

    @Override
    public void kill() {
        this.running = false;
        try {
            serverSocket.close();
        } catch (IOException e) {
            logger.error("Failed to close socket!");
        }
    }

    @Override
    public void close() {
        kill();
        clearCache();
		deleteTransferredKeys();
    }

    private boolean initializeKVServer() {
        logger.info("Starting KVServer...");
        try {
            serverSocket = new ServerSocket(port);
			stopped = true; // on init, only respond to ECS
            logger.info("Server listening on the port " + serverSocket.getLocalPort());
            return true;
        } catch (IOException e) {
            logger.error("Cannot open server socket!");
            if (e instanceof BindException) {
                logger.error("Port " + port + " is already bound!");
            }
            return false;
        }
    }

	@Override
	public ECSConsistentHash getMetadata() {
		return metadata;
	}

	@Override
	public synchronized boolean inServerKeyRange(String key) {
		return key.compareTo(start) >= 0 && key.compareTo(end) < 0;
	}

	@Override
	public boolean serverStopped() {
		return stopped;
	}

	@Override
	public boolean writeLocked() {
		return lock_writes;
	}

	@Override
	public void updateMetadata(String json) {
		// update metadata
		this.metadata.updateConsistentHash(json);

		// update the range this server is responsible for
		String key = getHostname() + ":" + getPort();
		IECSNode n = this.metadata.getNodeByKeyHash(key);
		this.setRange(n.getNodeHashRange());

		// TODO send ack message to ZK/ECS that metadata has been received

	}

	@Override
	public void moveData(String start, String end, String address, int port) {
		Map<String, String> allKVData = new HashMap<String, String>();

		try {
			allKVData = storage.getAllKVData();
		} catch (Exception e) {
			logger.error("Could not get all KV Pairs from storage");

			//TODO : send back error message to ECS
		}

		KVStore store = new KVStore(address, port);

		// try to connect to the server
		try {
			store.connect();
		} catch (Exception e) {
			logger.error("Could not connect to " + address + ":" + port + "to transfer data");

			//TODO : send back error message to ECS
		}

		for (Map.Entry<String, String> entry : allKVData.entrySet()) {
			String key = entry.getKey();
			String value = entry.getValue();
			String hashedKey = ECSNode.calculateHash(key);

			if (hashedKey.compareTo(start) >= 0 && hashedKey.compareTo(end) < 0) {
				// add to map of KV pairs to be deleted later
				this.toDelete.put(key, value);

				// send data to the server
				try {
					store.put(key, value);
				} catch (Exception e) {
					logger.error("Could not send <" + key + "," + value + "> to " + address + ":" + port);

					// TODO : send back error message to ECS
				}
			}
		}

		store.disconnect();
	}

	@Override
	public void rejectClientRequests() {
		logger.info("This server is now blocking client requests");
		stopped = true;
	}

	@Override
	public void acceptClientRequests() {
		logger.info("This server is now processing client requests");
		stopped = false;
	}

	@Override
	public void lockWrite() {
		logger.info("Write operations have been locked");
		lock_writes = true;

		// TODO Consider cases where there might be threads still writing or
		// waiting to write to storage
	}

	@Override
	public void unlockWrite() {
		// Delete keys that were transferred
		deleteTransferredKeys();
		logger.info("Write operations have been unlocked");
		lock_writes = false;

	}

	public synchronized void setRange(String[] range) {
		this.start = range[0];
		this.end = range[1];
	}

	private void deleteTransferredKeys() {
		for (String key : toDelete.keySet()) {
			putKV(key, "");
		}

		toDelete.clear();
	}

	private boolean checkValidKeyValue(String key, String value) {
		if (key == null || key.equals("") || key.contains(" ") || key.getBytes().length > maxKeyLength
				|| value.getBytes().length > maxValueLength) return false;
		else return true;
	}

	private boolean isRunning() {
		return this.running;
	}

	private void connectToZookeeper(String zkServer) throws IOException {
		CountDownLatch latch = new CountDownLatch(1);
		zk = new Zookeeper(zkServer, ECS.ZK_TIMEOUT, new Watcher() {
			public void process(WatchedEvent event) {
				if (event.getState().equals(Event.KeeperState.SyncConnected)) {
					latch.countDown();
				}
			}
		});

		try {
			// wait until we are connected
			latch.await();
		} catch (InterruptedException e) {
			logger.error("Error encountered while connecting to zookeeper: "
						+ e.getMessage());
		}
	}

	/* Process all messages from ECS to KVServer using zookeeper */
	@Override
	private void process(WatchedEvent event) {
		List<String> children;

		try {
			children = zk.getChildren(zkPath, false, null);
			String path = zkPath + "/" + children.get(0);

			if (children.isEmpty()) {
				// no message to process, just re-register watch
				zk.getChildren(zkPath, this, null);
			} else if (children.size() > 1) {
				String errorMsg = (name + " expects only one message, received "
									+ children.size());
				logger.error(errorMsg);
				zk.setData(path, errorMsg.getBytes(),
						   ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			} else {
				byte[] data = zk.getData(path, false, null);
				KVAdminMessage msg = new KVAdminMessage();
				String json = new String(data);
				msg.deserialize(json);

				switch (msg.getStatus()) {
					case START:
						acceptClientRequests();
						zk.delete(path, zk.exists(path, false).getVersion());
						break;
					case STOP:
						rejectClientRequests();
						zk.delete(path, zk.exists(path, false).getVersion());
						break;
					case SHUT_DOWN:
						rejectClientRequests();
						zk.delete(path, zk.exists(path, false).getVersion());
						close();
						break;
					case LOCK_WRITE:
						lockWrite();
						zk.delete(path, zk.exists(path, false).getVersion());
						break;
					case UNLOCK_WRITE:
						unlockWrite();
						zk.delete(path, zk.exists(path, false).getVersion());
						break;
					case MOVE_DATA:
						ArrayList<String> args = msg.getArgs();

						if (args.size() != 4) {
							String errorMsg = ("Incorrect number of args "
											   + "sent to MOVE_DATA command");
							zk.setData(path, errorMsg.getBytes(),
									   ZooDefs.Ids.OPEN_ACL_UNSAFE,
									   CreateMode.PERSISTENT);
						} else {
							moveData(args.get(0), args.get(1), args.get(2),
									Integer.parseInt(args.get(3)));
						}
						break;

				}

				// re-register watch
				if (isRunning()) {
					zk.getChildren(zkPath, this, null);
				}
			}
		} catch (KeeperException | InterruptedException e) {
            logger.error("Unable to process the watcher event");
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
			new LogSetup("logs/server.log", Level.ALL);

            if (args.length != 6) {
                System.out.println("Error! Invalid number of arguments!");
                System.out.println("Usage: KVServer <port> <cache size> " +
									"<caching strategy> <zkHost> <zkPort> " +
									"<name>!");
            } else if (argsAreOkay(args[0], args[1], args[2])) {

                int port = Integer.parseInt(args[0]);
                int cacheSize = Integer.parseInt(args[1]);
                String strategy = args[2];
				String zkHost = args[3];
				int zkPort = Integer.parseInt(args[4]);
				String name = args[5];

                new KVServer(port, cacheSize, strategy, zkHost, zkPort, name).start();
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
