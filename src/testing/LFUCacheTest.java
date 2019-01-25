package testing;

import org.junit.Test;

import junit.framework.TestCase;
import app_kvServer.cache.LFUCache;

public class LFUCacheTest extends CacheTest {

	public void setUp() throws Exception {
		cache = new LFUCache(3);
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
		cache.putKV("key", "new_value");
		cache.putKV("key_4", "val_4");

		assertTrue(cache.getSize() == 3);

		// "key_2" should be evicted
		assertFalse(cache.inCache("key_2"));
	}

	@Test
	public void testEvictCase2() throws Exception {
		cache.putKV("key", "");
		cache.putKV("key", "");
		cache.putKV("key_2", "");
		cache.putKV("key_3", "");
		cache.putKV("key_2", "");
		cache.putKV("key_4", "");

		assertTrue(cache.getSize() == 3);

		// "key_3" should be evicted
		assertFalse(cache.inCache("key_3"));
	}

	@Test
	public void testEvictCase3() throws Exception {
		cache.putKV("key", "");
		cache.putKV("key_2", "");
		cache.putKV("key_2", "");
		cache.putKV("key_3", "");
		cache.putKV("key_3", "");
		cache.putKV("key_4", "");


		assertTrue(cache.getSize() == 3);

		// "key" should be evicted (LRU breaks frequency tie)
		assertFalse(cache.inCache("key"));
	}
}
