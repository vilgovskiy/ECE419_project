package app_kvServer;

import ecs.*;
import shared.messages.JsonMessage;

public interface IKVServer {
    public enum CacheStrategy {
        None,
        LRU,
        LFU,
        FIFO
    };

    /**
     * Get the port number of the server
     * @return  port number
     */
    public int getPort();

    /**
     * Get the hostname of the server
     * @return  hostname of server
     */
    public String getHostname();

    /**
     * Get the cache strategy of the server
     * @return  cache strategy
     */
    public CacheStrategy getCacheStrategy();

    /**
     * Get the cache size
     * @return  cache size
     */
    public int getCacheSize();

    /**
     * Check if key is in storage.
     * NOTE: does not modify any other properties
     * @return  true if key in storage, false otherwise
     */
    public boolean inStorage(String key);

    /**
     * Check if key is in storage.
     * NOTE: does not modify any other properties
     * @return  true if key in storage, false otherwise
     */
    public boolean inCache(String key);

    /**
     * Get the value associated with the key
     * @return  value associated with key
     * @throws Exception
     *      when key not in the key range of the server
     */
    public JsonMessage getKV(String key);

    /**
     * Put the key-value pair into storage
     * @throws Exception
     *      when key not in the key range of the server
     */
    public JsonMessage putKV(String key, String value);

    /**
     * Clear the local cache of the server
     */
    public void clearCache();

    /**
     * Clear the storage of the server
     */
    public void clearStorage();

    /**
     * Starts running the server
     */
    public void run();

    /**
     * Abruptly stop the server without any additional actions
     * NOTE: this includes performing saving to storage
     */
    public void kill();

    /**
     * Gracefully stop the server, can perform any additional actions
     */
    public void close();

	/**
	 * Check if hashed key is within the key range the server is responsible for
	 */
	public boolean inServerKeyRange(String key);

	/**
	 * Check if the server is stopped (accepting client requests)
	 */
	public boolean serverStopped();

	/**
	 * Check if writes are locked
	 */
	public boolean writeLocked();

	/**
	 * Get the metadata from the server
	 */
	public ECSConsistentHash getMetadata();

	/**
	 * Update the metadata of the server
	 * @param json the metadata encoded in json
	 */
	public void updateMetadata(String json);

	/**
	 * Move all key value pairs within the given range to server
	 * @param start the start of the range
	 * @param end the end of the range (non-inclusive)
	 * @param address the address of the server
	 * @param port port number the server is listening on
	 */
	public void moveData(String start, String end, String address, int port);

	/**
	 * Stop processing client requests
	 */
	public void rejectClientRequests();

	/**
	 * Start processing client requests
	 */
	public void acceptClientRequests();

	/**
	 * Lock the server from write operations
	 */
	public void lockWrite();

	/**
	 * Unlock the server from write operations
	 */
	public void unlockWrite();
}
