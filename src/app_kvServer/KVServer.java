package app_kvServer;


import org.apache.log4j.Logger;
import server.KVClientConnection;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;

public class KVServer implements IKVServer {

    private static Logger logger = Logger.getRootLogger();

    private int port;
    private int cacheSize;
    private CacheStrategy strategy;
    private boolean running;
    private ServerSocket serverSocket;

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
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean inCache(String key) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public String getKV(String key) throws Exception {
        // TODO Auto-generated method stub
        return "";
    }

    @Override
    public void putKV(String key, String value) throws Exception {

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
}
