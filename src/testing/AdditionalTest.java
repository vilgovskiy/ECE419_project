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

public class AdditionalTest extends TestCase {
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
            } catch (IOException e) {
                logger.error("io exception during write", e);
            }
        }

        for (KVData updatedEntry : updateDataList) {
            try {
                long offset = storage.write(updatedEntry);
                indexMap.put(updatedEntry.getKey(), offset);
            } catch (IOException e) {
                logger.error("io exception during updated write", e);
            }
        }

        for (KVData readEntry : readDataList) {
            try {
                KVData foundEntry = storage.read(readEntry.getKey());
                assert(foundEntry.getKey().equals(readEntry.getKey()));
                assert(foundEntry.getValue().equals(readEntry.getValue()));
            } catch (IOException e) {
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
            } catch (IOException e) {
                logger.error("io exception during readFromIndex", e);
            }
        }
        storageFile.delete();
    }
}