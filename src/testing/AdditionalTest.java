package testing;

import org.junit.Test;

import junit.framework.TestCase;
import server.KVStorage;

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;

public class AdditionalTest extends TestCase {
	
	// TODO add your test cases, at least 3
	
	@Test
	public void testStub() {
		assertTrue(true);
	}

	@Test
	public void testFileRead(){

	}

	@Test
	public void testFileCreate(){
		KVStorage store = new KVStorage();
		try {
			store.writeToDisk("testWrite", "Attempt to write a text file to store directory.");
		} catch (FileNotFoundException fnfe){
			assertTrue(false);
		} catch (UnsupportedEncodingException uee){
			assertTrue(false);
		}
	}
}
