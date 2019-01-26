package testing;

import org.junit.Test;

import junit.framework.TestCase;
import server.cache.LRUCache;

public class LRUCacheTest extends CacheTest {

	public void setUp() throws Exception {
		cache = new LRUCache(3);
		cache.putKV("key", "val");
	}

	@Test
	public void testGetAndEvict() throws Exception {
		cache.putKV("key_2", "val_2");
		cache.putKV("key_3", "val_3");
		cache.getKV("key");
		cache.putKV("key_4", "val_4");

		assertTrue(cache.getSize() == 3);

		// "key_2" should be evicted
		assertFalse(cache.inCache("key_2"));
	}

	@Test
	public void testUpdateAndEvict() throws Exception {
		cache.putKV("key_2", "val_2");
		cache.putKV("key_3", "val_3");

		// key_2 goes to the back here
		cache.putKV("key_2", "new_val");

		// Evict out key and key_3
		cache.putKV("key_4", "val_4");
		cache.putKV("key_5", "val_5");

		assertTrue(cache.getSize() == 3);


		assertFalse(cache.inCache("key"));
		assertFalse(cache.inCache("key_3"));
	}
}
