package app_kvServer;


import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import server.IKVStorage;
import server.KVClientConnection;
import server.KVStorage;
import shared.messages.JsonMessage;
import shared.messages.KVMessage;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;

public class KVServer extends Thread implements IKVServer {

    private static Logger logger = Logger.getRootLogger();

    private int port;
    private int cacheSize;
    private CacheStrategy strategy;
    private boolean running;
    private ServerSocket serverSocket;
    private IKVStorage storage;

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
        storage = new KVStorage();

        logger.info("Creating an instance of the KV server");
    }

    @Override
    public int getPort() {
        return port;
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
        return storage.checkIfFileExists(key);
    }

    @Override
    public boolean inCache(String key) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public JsonMessage getKV(String key)  {
        JsonMessage responseMsg = new JsonMessage();
        responseMsg.setKey(key);
        try {
            String value = storage.getFileContents(key);
            responseMsg.setValue(value);
            responseMsg.setStatus(KVMessage.StatusType.GET_SUCCESS);
            logger.info("Succesfully retrieved value for key:" + key);
        } catch (FileNotFoundException e) {
            responseMsg.setStatus(KVMessage.StatusType.GET_ERROR);
            logger.info("Could not retrieve value for key:" + key + ". File doesnt exist");
        }
        return responseMsg;
    }

    @Override
    public JsonMessage putKV(String key, String value) {
        boolean fileAlreadyExists = inStorage(key);
        JsonMessage response = new JsonMessage();
        response.setKey(key);
        response.setValue(value);


        if (value.isEmpty()) {
            logger.info("Delete KV with key: " + key);
            if (fileAlreadyExists) {
                storage.deleteFile(key);
                logger.info("Successfully deleted KV with key:" + key);
                response.setStatus(KVMessage.StatusType.DELETE_SUCCESS);
            } else {
                response.setStatus(KVMessage.StatusType.DELETE_ERROR);
                logger.info("Unable to delete key:" + key + ". File doesn't exist!");
            }
        } else {
            response.setStatus(KVMessage.StatusType.PUT_ERROR);
            try {
                storage.writeToDisk(key, value);
                if (fileAlreadyExists){
                    response.setStatus(KVMessage.StatusType.PUT_UPDATE);
                    logger.info("Successfully updated key:" + key + " with value:" + value);
                } else {
                    response.setStatus(KVMessage.StatusType.PUT_SUCCESS);
                    logger.info("Successfully inserted key:" + key + " with value:" + value);
                }

            } catch (FileNotFoundException e) {
                logger.error("Somehow file could not be found");
            } catch (UnsupportedEncodingException e) {
                logger.error("Unsupported encoding");
            }
        }
        return response;
    }

    @Override
    public void clearCache() {
        // TODO Auto-generated method stub
    }

    @Override
    public void clearStorage() {
        // TODO Auto-generated method stub
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
        logger.info("Starting the KVSertver");
        try {
            serverSocket = new ServerSocket(port);
            logger.info("Server listening on the port" + serverSocket.getLocalPort());
            return true;
        } catch (IOException e) {
            logger.error("Cannot open server socket:");
            if (e instanceof BindException) {
                logger.error("Port " + port + " is already bound!");
            }
            return false;
        }
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
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        } catch (NumberFormatException nfe) {
            System.out.println("Error! Invalid argument <port> or <cache_size>! Not a number!");
            System.out.println("Usage: Server <port>!");
            System.exit(1);
        }
    }
}
