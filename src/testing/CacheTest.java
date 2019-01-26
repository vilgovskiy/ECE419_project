package testing;

import org.junit.Test;

import junit.framework.TestCase;
import server.cache.Cache;

public class CacheTest extends TestCase {

	protected Cache cache;

	@Test
	public void testCacheSizeIncreasesOnInsert() throws Exception {
		int prevSize = cache.getSize();
		cache.putKV("test_key", "test_val");
		assertTrue(cache.getSize() == prevSize + 1);
	}

	@Test
	public void testCacheSizeDecreasesOnDelete() throws Exception {
		int prevSize = cache.getSize();
		cache.putKV("key", null);
		assertTrue(cache.getSize() == prevSize - 1);
	}

	@Test
	public void testInCacheSuccess() {
		assertTrue(cache.inCache("key"));
	}

	@Test
	public void testInCacheFailure() {
		assertFalse(cache.inCache("random_key"));
	}

	@Test
	public void testClearCache() {
		cache.clearCache();
		assertTrue(cache.getSize() == 0);
		assertFalse(cache.inCache("key"));
	}

	@Test
	public void testGetKV() throws Exception {
		assertTrue(cache.getKV("key") == "val");
	}

	@Test
	public void testInsert() throws Exception {
		cache.putKV("new_key", "new_value");
		assertTrue(cache.getKV("new_key") == "new_value");
	}

	@Test
	public void testUpdate() throws Exception {
		cache.putKV("key", "new_value");
		assertTrue(cache.getKV("key") == "new_value");
	}

	@Test
	public void testDelete() throws Exception {
		cache.putKV("key", null);
		assertFalse(cache.inCache("key"));
	}

	@Test
	public void testStub() {
		assertTrue(true);
	}
}
