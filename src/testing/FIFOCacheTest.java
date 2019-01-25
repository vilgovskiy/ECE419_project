package testing;

import org.junit.Test;

import junit.framework.TestCase;
import app_kvServer.cache.FIFOCache;

public class FIFOCacheTest extends CacheTest {

	public void setUp() throws Exception {
		cache = new FIFOCache(3);
		cache.putKV("key", "val");
	}

	@Test
	public void testEviction() throws Exception {
		cache.putKV("key_2", "val_2");
		cache.putKV("key_3", "val_3");
		cache.putKV("key_4", "val_4");

		assertTrue(cache.getSize() == 3);

		// "key" should be evicted
		assertFalse(cache.inCache("key"));
	}

	@Test
	public void testDeleteAndEvict() throws Exception {
		cache.putKV("key_2", "val_2");
		cache.putKV("key_3", "val_3");
		cache.putKV("key", null);
		cache.putKV("key", "val");
		cache.putKV("key_4", "val_4");

		assertTrue(cache.getSize() == 3);

		// "key_2" should be evicted
		assertFalse(cache.inCache("key_2"));
	}
}
