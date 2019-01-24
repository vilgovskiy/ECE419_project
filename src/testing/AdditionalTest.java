package testing;

import org.junit.Test;

import junit.framework.TestCase;
import server.KVStorage;

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;

import client.KVStore;
import shared.messages.JsonMessage;
import shared.messages.KVMessage;

public class AdditionalTest extends TestCase {

    // TODO add your test cases, at least 3

    @Test
    public void testStub() {
        assertTrue(true);
    }

    @Test
    public void testFileRead() {

    }

    @Test
    public void testFileCreate() {
        KVStorage store = new KVStorage();
        try {
            store.writeToDisk("testWrite", "Attempt to write a text file to store directory.");
        } catch (FileNotFoundException fnfe) {
            assertTrue(false);
        } catch (UnsupportedEncodingException uee) {
            assertTrue(false);
        }
        store.deleteFile("testWrite");
    }

    @Test
    public void testGetFileContents() {
        KVStorage store = new KVStorage();
        try {
            store.writeToDisk("read_file_test", "Testing weather KVStore can read the file");
        } catch (FileNotFoundException fnfe) {
            assertTrue(false);
        } catch (UnsupportedEncodingException uee) {
            assertTrue(false);
        }

        String funcOut = "";
        try {
            funcOut = store.getFileContents("read_file_test");
        } catch (FileNotFoundException e) {
            assertTrue(false);
        }
        store.deleteFile("read_file_test");
        assertEquals("Testing weather KVStore can read the file", funcOut);

    }

    @Test
    public void testGetFileContentsNonExistingFile() {
        KVStorage store = new KVStorage();

        FileNotFoundException ex = null;
        try {
            store.getFileContents("someFile");
        } catch (FileNotFoundException e) {
            ex = e;
        }
        assertNotNull(ex);
    }


    @Test
    public void testFileOverwrite() {
        KVStorage store = new KVStorage();
        try {
            store.writeToDisk("overwrittenFile", "File_contents 1");
        } catch (FileNotFoundException fnfe) {
            assertTrue(false);
        } catch (UnsupportedEncodingException uee) {
            assertTrue(false);
        }

        String funcOut = "";
        try {
            funcOut = store.getFileContents("overwrittenFile");
        } catch (FileNotFoundException e) {
            assertTrue(false);
        }
        assertEquals("File_contents 1", funcOut);

        try {
            store.writeToDisk("overwrittenFile", "File_contents 2");
        } catch (FileNotFoundException fnfe) {
            assertTrue(false);
        } catch (UnsupportedEncodingException uee) {
            assertTrue(false);
        }

        try {
            funcOut = store.getFileContents("overwrittenFile");
        } catch (FileNotFoundException e) {
            assertTrue(false);
        }
        assertEquals("File_contents 2", funcOut);
    }

}

	@Test
	public void testJsonMessage() {
		JsonMessage msg = new JsonMessage(KVMessage.StatusType.PUT, "sampleKey", "sampleValue");
		String strMsg = msg.serialize();
		System.out.println(strMsg);
		JsonMessage deserializedMsg = new JsonMessage();
		deserializedMsg.deserialize(strMsg);

		assert(deserializedMsg.getKey().equals("sampleKey"));
		assert(deserializedMsg.getKey().equals("sampleValue"));
		assert(deserializedMsg.getStatus() == (KVMessage.StatusType.PUT));
	}
}