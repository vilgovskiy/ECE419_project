package server.storage;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.RandomAccessFile;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public class KVStorage implements IKVStorage {

    private static Logger logger = Logger.getRootLogger();
    private static final String fileExt = ".db";

    private String storageFilePath;
    private String storageFileName;
    private File storageFile;
    private long currLength;

    // Constructor that initialize KVStorage instance with new file
    public KVStorage(String fileName) {
        this.storageFileName = fileName;
        this.storageFilePath = fileName + fileExt;
        openStorageFile();
        this.currLength = 0;
    }

    // Constructor that initialize KVStorage instance with existing file
    public KVStorage(String filePath, long currLength) {
        this.storageFilePath = filePath;
        this.currLength = currLength;
        this.storageFileName = filePath.split("/")[2].replaceAll(fileExt, "");
    }

    private void openStorageFile() {
        if (storageFile == null) {
            try {
                storageFile = new File(storageFilePath);
                boolean createFile = storageFile.createNewFile();
                if (!createFile) logger.info("storage file " + storageFilePath + " exists already");
                else logger.info("storage file " + storageFilePath + " created");
            } catch (IOException e) {
                logger.error("cannot create data file" + storageFilePath);
            }
        }
    }

    public long getCurrLength() {
        return currLength;
    }

    public String getStorageFilePath() {
        return storageFilePath;
    }

    @Override
    public synchronized long write(KVData kvData) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(storageFilePath, "rw");
        raf.seek(raf.length());
        raf.write(kvData.toByteArray());
        currLength = raf.length();  // the currentFileLength will be the file length after the append
        raf.close();
        return currLength; // return the file length before appending as index for cache purpose
    }

    // read the entire file to find the key
    @Override
    public KVData read(String key) throws IOException {
        KVData foundEntry = new KVData();
        boolean found = false;
        RandomAccessFile raf = new RandomAccessFile( storageFilePath, "r");
        long currOffset = raf.length(); // start reading the file from the current offset
        byte[] keySizeBytes = new byte[4];
        byte[] valueSizeBytes = new byte[4];

        while (currOffset >= 0) {
            currOffset -= 8;
            raf.seek(currOffset);
            raf.read(keySizeBytes);
            raf.read(valueSizeBytes);

            int keySize = ByteBuffer.wrap(keySizeBytes).getInt();
            int valueSize = ByteBuffer.wrap(valueSizeBytes).getInt();

            byte[] keyBytes = new byte[keySize];
            currOffset -= (keySize + valueSize);
            raf.seek(currOffset);
            raf.read(keyBytes);
            String keyString = new String(keyBytes, Charset.forName("UTF-8"));

            if (keyString.equals(key)) {
                byte[] valueBytes = new byte[valueSize];
                raf.read(valueBytes);
                String value = new String(valueBytes, Charset.forName("UTF-8"));
                foundEntry.setKey(key);
                foundEntry.setValue(value);
                found = true;
                break;
            }
        }
        raf.close();
        return found ? foundEntry : null; // if found the key-value we return found entry, else return null
    }

    // Directly read the file from index using seek, can only be called by get in cache operation
    @Override
    public KVData readFromIndex(String key, long index) throws IOException {
        KVData foundEntry = new KVData();
        boolean found = false;
        byte[] keySizeBytes = new byte[4];
        byte[] valueSizeBytes = new byte[4];

        RandomAccessFile raf = new RandomAccessFile(storageFilePath, "r");
        index -= 8;
        raf.seek(index);
        raf.read(keySizeBytes);
        raf.read(valueSizeBytes);

        int keySize = ByteBuffer.wrap(keySizeBytes).getInt();
        int valueSize = ByteBuffer.wrap(valueSizeBytes).getInt();

        byte[] keyBytes = new byte[keySize];
        index -= (keySize + valueSize);
        raf.seek(index);
        raf.read(keyBytes);
        String keyString = new String(keyBytes, Charset.forName("UTF-8"));

        if (keyString.equals(key)) {
            byte[] valueBytes = new byte[valueSize];
            raf.read(valueBytes);
            String value = new String(valueBytes, Charset.forName("UTF-8"));
            foundEntry.setKey(key);
            foundEntry.setValue(value);
            found = true;
        } else {
            logger.error("cannot find key " + key + " from index");
        }
        raf.close();
        return found ? foundEntry : null;
    }
}

class KVStorageReader implements Runnable {
    private static Logger logger = Logger.getRootLogger();
    KVStorage kvStorageInstance = null;
    String targetKey;
    KVData foundEntry = null;

    public KVStorageReader(KVStorage kvStorage, String key) {
        this.kvStorageInstance = kvStorage;
        this.targetKey = key;
    }

    @Override
    public void run() {
        try {
            foundEntry = kvStorageInstance.read(targetKey);
        } catch(IOException e) {
            logger.error("IOException during getting key from file: " + kvStorageInstance.getStorageFilePath());
        }
    }
}
