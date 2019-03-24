package testing;

import org.junit.Test;

import junit.framework.TestCase;
import server.storage.*;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import logger.LogSetup;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

public class KVStorageTests extends TestCase {
    private static Logger logger = Logger.getRootLogger();

    static {
        try {
            new LogSetup("logs/testing/test.log", Level.INFO);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testKVStorage() {
        KVStorage storage = KVStorage.getInstance("");
        File storageFile = new File("_data");
        assert(storageFile.exists());

        KVData data1 = new KVData("key", "value");
        KVData data2 = new KVData("key1", "");

        KVData foundEntry = new KVData();
        try {
            storage.write(data1);
            storage.write(data2);
            foundEntry = storage.read("key1");
        } catch (Exception e) {}

        assert(foundEntry.getValue().equals(""));
        storageFile.delete();
    }


}
