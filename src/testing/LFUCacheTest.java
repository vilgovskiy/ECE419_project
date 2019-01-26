package testing;

import org.junit.Test;

import junit.framework.TestCase;
import server.cache.LFUCache;

public class LFUCacheTest extends CacheTest {

	public void setUp() {
		cache = new LFUCache(3);
		cache.putKV("key", "val");
	}

	@Test
	public void testGetAndEvict() {
		cache.putKV("key_2", "val_2");
		cache.putKV("key_3", "val_3");
		cache.getKV("key");
		cache.putKV("key_4", "val_4");

		assertTrue(cache.getSize() == 3);

		// "key_2" should be evicted
		assertFalse(cache.inCache("key_2"));
	}

	@Test
	public void testUpdateAndEvict() {
		cache.putKV("key_2", "val_2");
		cache.putKV("key_3", "val_3");
		cache.putKV("key", "new_value");
		cache.putKV("key_4", "val_4");

		assertTrue(cache.getSize() == 3);

		// "key_2" should be evicted
		assertFalse(cache.inCache("key_2"));
	}

	@Test
	public void testEvictCase2() {
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
	public void testEvictCase3() {
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
