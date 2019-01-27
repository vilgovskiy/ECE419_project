package app_kvServer;


import logger.LogSetup;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import server.cache.*;
import server.storage.*;
import server.KVClientConnection;
import shared.messages.JsonMessage;
import shared.messages.KVMessage;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.SocketException;
import java.net.Socket;

public class KVServer extends Thread implements IKVServer {

    private static Logger logger = Logger.getRootLogger();
    private static final int maxKeyLength = 20;
    private static final int maxValueLength = 120 * 1024;

    private int port;
    private int cacheSize;
    private CacheStrategy strategy;
    private boolean running;
    private ServerSocket serverSocket;
    private IKVStorage storage;
	private Cache cache;


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
        storage = KVStorage.getInstance();

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
        logger.info("creating an instance of the KV server...");
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
                    responseMsg.setValue(value);
                    responseMsg.setStatus(KVMessage.StatusType.GET_SUCCESS);
                    cache.putKV(key, value);
                    logger.info("GET_SUCCESS for <key: " + key + ", value: " + foundEntry.getValue() + ">");
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
        storage.clearStorage();
		clearCache();
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
					logger.error("Error! socket exception");
				} catch (IOException e) {
                    logger.error("Error! " +
                            "Unable to establish connection. \n", e);
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
        //TODO Might also need to wipe cache and remove all connections
    }

    private boolean initializeKVServer() {
        logger.info("Starting KVServer...");
        try {
            serverSocket = new ServerSocket(port);
            logger.info("Server listening on the port " + serverSocket.getLocalPort());
            return true;
        } catch (IOException e) {
            logger.error("Cannot open server socket:");
            if (e instanceof BindException) {
                logger.error("Port " + port + " is already bound!");
            }
            return false;
        }
    }

    private boolean checkValidKeyValue(String key, String value) {
        if (key == null || key.equals("") || key.contains(" ") || key.getBytes().length > maxKeyLength
                || value.getBytes().length > maxValueLength) return false;
        else return true;
    }

    private boolean isRunning() {
        return this.running;
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

            if (args.length != 3) {
                System.out.println("Error! Invalid number of arguments!");
                System.out.println("Usage: KVServer <port> <cahche size> <caching strategy>!");
            } else if (argsAreOkay(args[0], args[1], args[2])) {

                int port = Integer.parseInt(args[0]);
                int cacheSize = Integer.parseInt(args[1]);
                String strategy = args[2];

                new KVServer(port, cacheSize, strategy).start();
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
