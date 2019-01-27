package server.storage;

import org.apache.log4j.Logger;

import java.util.List;
import java.util.PriorityQueue;
import java.io.IOException;

public class StorageReader implements Runnable {
    private static Logger logger = Logger.getRootLogger();
    private static PriorityQueue<Integer> fileNamePQueue;
    private static List<String> foundValueList;
    private static String targetKey;

    private KVStorage storage;
    private int fileIndex;

    public StorageReader(KVStorage storage){
        this.storage = storage;
        this.fileIndex = storage.getStorageFileIndex();
    }

    public static void setTargetKey(String targetKey) {
        StorageReader.targetKey = targetKey;
    }

    @Override
    public void run() {
        ReadResult result = new ReadResult(this.fileIndex);
        try {
            KVData foundEntry = this.storage.read(targetKey);

        } catch (IOException e) {
            logger.error("IO Exception during read");
        }



    }
}

class ReadResult {
    int fileIndex;
    String foundValue;
    long foundOffset;

    public ReadResult(int fileIndex) {
        this.fileIndex = fileIndex;
    }

    public void setFoundValue(String foundValue) {
        this.foundValue = foundValue;
    }

    public void setFoundOffset(long foundOffset) {
        this.foundOffset = foundOffset;
    }
}
