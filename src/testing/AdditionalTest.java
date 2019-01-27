package testing;

import org.junit.Test;

import junit.framework.TestCase;
<<<<<<< HEAD
import server.KVStorage;

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;

import client.KVStore;
import shared.messages.JsonMessage;
import shared.messages.KVMessage;
=======
import server.storage.*;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import logger.LogSetup;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
>>>>>>> single_storage

public class AdditionalTest extends TestCase {
    private static Logger logger = Logger.getRootLogger();

<<<<<<< HEAD
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
=======
    static {
        try {
            new LogSetup("logs/testing/test.log", Level.INFO);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testKVStorage() {
        KVStorage storage = KVStorage.getInstance();
        File storageFile = new File("data");
        assert(storageFile.exists());

        List<KVData> dataList = new ArrayList<>();
        List<KVData> updateDataList = new ArrayList<>();
        List<KVData> readDataList = new ArrayList<>();
        HashMap<String, Long> indexMap = new HashMap<>();

        for (int i = 1; i <= 20; i++) {
            String key = "key-" + i;
            String value = "value-" + i;
            KVData entry = new KVData(key, value);
            dataList.add(entry);

            if (i%4 == 0) {
                KVData updateEntry = new KVData(key, "updateValue-"+i);
                updateDataList.add(updateEntry);
                readDataList.add(updateEntry);
            } else {
                readDataList.add(entry);
            }
        }

        for (KVData entry : dataList) {
            try {
                long offset = storage.write(entry);
                indexMap.put(entry.getKey(), offset);
            } catch (Exception e) {
                logger.error("io exception during write", e);
            }
        }

        for (KVData updatedEntry : updateDataList) {
            try {
                long offset = storage.write(updatedEntry);
                indexMap.put(updatedEntry.getKey(), offset);
            } catch (Exception e) {
                logger.error("io exception during updated write", e);
            }
>>>>>>> single_storage
        }
        assertEquals("File_contents 1", funcOut);

<<<<<<< HEAD
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

	@Test
	public void testJsonMessage() {
		JsonMessage msg = new JsonMessage(KVMessage.StatusType.PUT, "sampleKey", "sampleValue");
		String strMsg = msg.serialize();
		System.out.println(strMsg);
		JsonMessage deserializedMsg = new JsonMessage();
		deserializedMsg.deserialize(strMsg);

		assert(deserializedMsg.getKey().equals("sampleKey"));
		assert(deserializedMsg.getValue().equals("sampleValue"));
		assert(deserializedMsg.getStatus() == (KVMessage.StatusType.PUT));
	}

	@Test
    public void testFileExistsOnNonExistingFile(){
        KVStorage store = new KVStorage();
        boolean res = store.checkIfFileExists("ThisFileShouldntExist");
        assertFalse(res);
=======
        for (KVData readEntry : readDataList) {
            try {
                KVData foundEntry = storage.read(readEntry.getKey());
                assert(foundEntry.getKey().equals(readEntry.getKey()));
                assert(foundEntry.getValue().equals(readEntry.getValue()));
            } catch (Exception e) {
                logger.error("io exception during read", e);
            }
        }

        for (int i = 1; i <= 20; i++) {
            String key = "key-"+i;
            String value = "value-"+i;
            if (i%4==0) value = "updateValue-"+i;
            long offset = indexMap.get(key);
            try {
                KVData foundEntry = storage.readFromIndex(key, offset);
                assert(foundEntry.getKey().equals(key));
                assert(foundEntry.getValue().equals(value));
            } catch (Exception e) {
                logger.error("io exception during readFromIndex", e);
            }
        }
        storageFile.delete();
>>>>>>> single_storage
    }


}
