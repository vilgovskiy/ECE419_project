package server.cache;

import java.util.HashMap;
import server.cache.util.DLL;
import server.cache.util.Node;

public abstract class Cache {
	protected int size;
	protected int maxSize;

	public Cache(int maxSize) {
		this.size = 0;
		this.maxSize = maxSize;
	}

	/**
	 * Gets the size of the cache
	 * @return size
	 */
	public synchronized int getSize() {
		return this.size;
	}

	/**
	 * Retrieve the value for the given key
	 * @return value associated with key
	 * @throws RuntimeException if key not in cache
	 */
	 public abstract String getKV(String key) throws RuntimeException;

	/**
	 * If value is null, delete key value pair associated with key
	 * If key is in cache, update the value associated with key
	 * Else insert KV pair in cache, evict KV pair if cache is full
	 * @throws RuntimeException if value is null, but key is not in cache
	 */
	 public abstract void putKV(String key, String value) throws RuntimeException;

	/**
	 * Check if key is in cache
	 * @return true if key in cache, else false
	 */
	 public abstract boolean inCache(String key);

	/**
	 * Clear all KV pairs from cache
	 */
	 public abstract void clearCache();

}
