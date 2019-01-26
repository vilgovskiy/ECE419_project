package testing;

import org.junit.Test;

import junit.framework.TestCase;
import server.storage.KVStorage;
import server.storage.KVData;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import logger.LogSetup;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

public class AdditionalTest extends TestCase {

    @Test
    public void testKVStorage() {

        try {
            new LogSetup("logs/test.log", Level.OFF);
        } catch(IOException e) {}

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
    }
}