package testing;

import org.junit.Test;

import junit.framework.TestCase;
import server.storage.*;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import logger.LogSetup;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

public class AdditionalTest extends TestCase {

    static {
        try {
            new LogSetup("logs/testing/test.log", Level.INFO);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testKVStorageManager() {
        KVStorageManager manager = KVStorageManager.getInstance();
        List<KVData> firstDataList = new ArrayList<>();
        List<KVData> secondDataList = new ArrayList<>();
        List<KVData> lastDataList = new ArrayList<>();
        List<Long> indexList = new ArrayList<>();

        for (int i = 0 ; i < 10000; i++) {
            firstDataList.add(new KVData("key"+i, "value"+1));
        }



        for (KVData data : dataList) {
            try {
                indexList.add(manager.put(data));
            } catch (IOException e) {
                System.out.println("IO Exception during write!");
            }
        }

        for (KVData data : dataList) {
            try {
                KVData foundEntry = manager.get(data.getKey());
                assert(foundEntry.getValue().equals(data.getValue()));
                assert(foundEntry.getKey().equals(data.getKey()));
            } catch (Exception e) {
                System.out.println("EXCEIOTN!");
            }
        }
    }

    /*@Test
    public void testKVStorage() {

        KVStorage storage = new KVStorage("one");
        File storageFile = new File("/Users/brucechenchen/github/ECE419_project/one.db");
        assert(storageFile.exists());

        List<KVData> dataList = new ArrayList<>();
        List<Long> indexList = new ArrayList<>();
        dataList.add(new KVData("key1", "value1"));
        dataList.add(new KVData("key2", "value2"));
        dataList.add(new KVData("object", "storageValue"));
        dataList.add(new KVData("hello", "world"));


        for (KVData data : dataList) {
            try {
                indexList.add(storage.write(data));
            } catch (IOException e) {
                System.out.println("IO Exception during write!");
            }
        }

        for (KVData data : dataList) {
            try {
                KVData foundEntry = storage.read(data.getKey());
                assert(foundEntry.getKey().equals(data.getKey()));
                assert(foundEntry.getValue().equals(data.getValue()));
            } catch(IOException e) {
                System.out.println("IO Exception during read!");
            }
        }

        int i = 0;
        for (long index : indexList) {
            try {
                KVData foundEntry = storage.readFromIndex(dataList.get(i).getKey(), index);
                assert(foundEntry.getKey().equals(dataList.get(i).getKey()));
                assert(foundEntry.getValue().equals(dataList.get(i).getValue()));
                i++;
            } catch (Exception e) {
                System.out.println("Exception during read from index!");
            }
        }
    }*/
}