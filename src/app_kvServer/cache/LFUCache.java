package app_kvServer.cache;

import java.util.HashMap;
import app_kvServer.cache.util.*;

public class LFUCache extends Cache {
	private DLL frequencies;
	private HashMap<String, LFUItemNode> hmap;

	public LFUCache(int maxSize) {
		super(maxSize);
		frequencies = new DLL();
		hmap = new HashMap<String, LFUItemNode>();
	}

	private void reorder(String key) {
		LFUItemNode n = hmap.get(key);
		LFUFreqNode fn = n.freqNode;
		LFUFreqNode nextFn = (LFUFreqNode) fn.next;

		if(nextFn != null && fn.freq + 1 == nextFn.freq) {
			fn.queue.delete(n);
			nextFn.queue.insertRear(n);

			if(fn.queue.getSize() == 0) {
				frequencies.delete(fn);
			}
		} else {
			if(fn.queue.getSize() == 1) {
				fn.freq = fn.freq + 1;
			} else {
				LFUFreqNode newFNode = new LFUFreqNode(fn.freq + 1);
				n.freqNode = newFNode;
				fn.queue.delete(n);
				newFNode.queue.insertRear(n);
				frequencies.insertRight(fn, newFNode);
			}
		}

	}

	@Override
	public synchronized String getKV(String key) throws Exception {
		if(inCache(key)) {
			// Move KV pair to the next frequency node
			reorder(key);
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
				LFUItemNode toDelete = hmap.get(key);
				LFUFreqNode fn = toDelete.freqNode;
				fn.queue.delete(toDelete);

				// If there are no more KV pairs with this frequency,
				// delete this frequency node
				if(fn.queue.getSize() == 0) {
					frequencies.delete(fn);
				}

				hmap.remove(key);
				size--;
			} else {
				// Move KV pair to the back of the queue
				reorder(key);

				// update value in KV pair
				hmap.get(key).value = value;
			}
		} else {
			if(value == null) {
				throw new Exception("Unable to delete, key not in cache");
			} else {
				if(size == maxSize) {
					// Find the KV pairs with the min freq
					LFUFreqNode front = (LFUFreqNode) frequencies.getHead();
					DLL minFreqs = front.queue;

					// Delete the Least Recently Used
					LFUItemNode evicted = (LFUItemNode) minFreqs.deleteFront();

					// If no more KV pairs with min freq, delete freq node
					if(minFreqs.getSize() == 0) {
						frequencies.delete(front);
					}

					hmap.remove(evicted.key);
					size--;
				}

				// insert KV pair
				LFUItemNode newNode = new LFUItemNode(key, value);
				hmap.put(key, newNode);

				LFUFreqNode front = (LFUFreqNode) frequencies.getHead();

				if(front != null && front.freq == 1) {
					front.queue.insertRear(newNode);
					newNode.freqNode = front;
				} else {
					LFUFreqNode newFNode = new LFUFreqNode(1);
					newNode.freqNode = newFNode;
					newFNode.queue.insertRear(newNode);
					frequencies.insertFront(newFNode);
				}

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
		hmap = new HashMap<String, LFUItemNode>();
		frequencies = new DLL();
		size = 0;
	}
}
