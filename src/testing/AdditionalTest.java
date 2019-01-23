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

}