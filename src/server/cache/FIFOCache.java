package server.cache;

import java.util.HashMap;
import server.cache.util.DLL;
import server.cache.util.Node;

public class FIFOCache extends Cache {
	private DLL queue;
	private HashMap<String, Node> hmap;

	public FIFOCache(int maxSize) {
		super(maxSize);
		queue = new DLL();
		hmap = new HashMap<String, Node>();
	}

	@Override
	public synchronized String getKV(String key) throws RuntimeException {
		if(inCache(key)) {
			return hmap.get(key).value;
		} else {
			throw new RuntimeException("Attempted to access key not in cache");
		}
	}

	@Override
	public synchronized void putKV(String key, String value) throws RuntimeException {
		if(inCache(key)) {
			if(value == null) {
				// delete KV pair
				queue.delete(hmap.get(key));
				hmap.remove(key);
				size--;
			} else {
				// update value in KV pair
				hmap.get(key).value = value;
			}
		} else {
			if(value == null) {
				throw new RuntimeException("Attemped to delete a key not in cache");
			} else {
				if(size == maxSize) {
					// evict KV pair
					Node evicted = queue.deleteFront();
					hmap.remove(evicted.key);
					size--;
				}

				// insert KV pair
				Node newNode = new Node(key, value);
				hmap.put(key, newNode);
				queue.insertRear(newNode);
				size++;
			}
		}
	}

  	@Override
	public synchronized boolean inCache(String key) {
		return hmap.containsKey(key);
	}

	@Override
	public synchronized void clearCache() {
		hmap = new HashMap<String, Node>();
		queue = new DLL();
		size = 0;
	}
}
