package server.storage;

import org.apache.log4j.Logger;

import java.util.Objects;
import java.util.PriorityQueue;
import java.io.IOException;

public class StorageReader implements Runnable {
    private static Logger logger = Logger.getRootLogger();
    private static PriorityQueue<ReadResult> fileIndexPQueue = new PriorityQueue<>();
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

    public static ReadResult getFinalReadResult() {
        return fileIndexPQueue.remove();
    }

    @Override
    public void run() {
        try {
            KVData foundEntry = this.storage.read(targetKey);
            ReadResult result = new ReadResult(this.fileIndex);
            result.setFoundValue(foundEntry.getValue());
            result.setFoundOffset(foundEntry.getOffset());
            fileIndexPQueue.add(result);
        } catch (IOException e) {
            logger.error("IO Exception during read");
        }
    }
}

class ReadResult implements Comparable<ReadResult> {
    int fileIndex;
    String foundValue;
    long foundOffset;

    public ReadResult(int fileIndex) {
        this.fileIndex = fileIndex;
        this.foundValue = "";
        this.foundOffset = -1;
    }

    public void setFoundValue(String foundValue) {
        this.foundValue = foundValue;
    }

    public void setFoundOffset(long foundOffset) {
        this.foundOffset = foundOffset;
    }

    public int getFileIndex() { return fileIndex; }

    public String getFoundValue() { return foundValue; }

    public long getFoundOffset() { return foundOffset; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || this.getClass() != o.getClass()) return false;
        ReadResult result = (ReadResult) o;
        return result.fileIndex == fileIndex &&
                foundValue.equals(result.getFoundValue()) &&
                result.foundOffset == foundOffset;
    }

    @Override
    public int compareTo(ReadResult result) {
        if (this.getFileIndex() > result.getFileIndex()) {
            return -1;
        } else if (this.getFileIndex() < result.getFileIndex()) {
            return 1;
        } else {
            return 0;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileIndex, foundValue, foundOffset);
    }
}
