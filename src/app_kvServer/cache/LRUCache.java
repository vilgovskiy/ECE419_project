package cache;

import java.util.HashMap;
import cache.structures.DLL;
import cache.structures.Node;

public class LRUCache extends Cache {
	HashMap<String, Node> hmap;
	DLL q;

	public LRUCache(int maxSize) {
		super(maxSize);
		hmap = new HashMap<String, Node>();
		q = new DLL();
	}

	private void moveNodeToRear(String key) {
		Node n = hmap.get(key);
		q.delete(n);
		q.insertRear(n);
	}

	@Override
	public synchronized String getKV(String key) throws Exception {
		if(inCache(key)) {
			// Move KV pair to back of queue
			moveNodeToRear(key);
			return hmap.get(key).value;
		} else {
			throw new Exception("Key not in cache");
		}
	}

	@Override
	public synchronized void putKV(String key, String value) throws Exception {
		if(inCache(key)) {
			if(value == null) {
				// delete KV pair
				q.delete(hmap.get(key));
				hmap.remove(key);
				size--;
			} else {
				// update value in KV pair
				moveNodeToRear(key);
				hmap.get(key).value = value;
			}
		} else {
			if(value == null) {
				throw new Exception("Unable to delete, key not in cache");
			} else {
				if(size == maxSize) {
					// evict KV pair that is in front of the queue
					Node evicted = q.deleteFront();
					hmap.remove(evicted.key);
					size--;
				}

				// insert KV pair
				Node newNode = new Node(key, value);
				hmap.put(key, newNode);
				q.insertRear(newNode);
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
		q = new DLL();
		size = 0;
	}
}
