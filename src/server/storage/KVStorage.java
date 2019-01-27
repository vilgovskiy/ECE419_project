package server.storage;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.RandomAccessFile;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class KVStorage implements IKVStorage {
    private static Logger logger = Logger.getRootLogger();
    private static KVStorage storageInstance = null;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private String dataFilePath = "data";
    private long currLength;

    private KVStorage() {
        createStorageFile();
        this.currLength = 0;
    }

    public static KVStorage getInstance() {
        if (storageInstance == null) storageInstance = new KVStorage();
        return storageInstance;
    }

    private void createStorageFile() {
        logger.info("creating data file at " + dataFilePath + "...");
        File storageFile = new File(dataFilePath);
        if (!storageFile.exists()) {
            try {
                boolean createFile = storageFile.createNewFile();
                if (!createFile) logger.info("data file " + dataFilePath + " exists already");
                else logger.info("data file " + dataFilePath + " created");
            } catch (IOException e) {
                logger.error("cannot create data file" + dataFilePath, e);
            }
        }
    }

    public long getCurrLength() {
        return currLength;
    }

    public String getDataFilePath() {
        return dataFilePath;
    }

    @Override
    public void write(KVData kvData) throws IOException {
        rwLock.writeLock().lock();
        try {
            RandomAccessFile raf = new RandomAccessFile(dataFilePath, "rw");
            raf.seek(raf.length());
            raf.write(kvData.toByteArray());
            currLength = raf.length();
            raf.close();
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public KVData read(String key) throws IOException {
        rwLock.readLock().lock();
        try {
            KVData foundEntry = new KVData();
            boolean found = false;
            RandomAccessFile raf = new RandomAccessFile( dataFilePath, "r");
            long currOffset = raf.length();
            if (currOffset == 0) {
                return null;
            }

            byte[] keySizeBytes = new byte[4];
            byte[] valueSizeBytes = new byte[4];

            while (currOffset > 0) {
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
        } finally {
            rwLock.readLock().unlock();
        }
    }

    // Directly read the file from index using seek, can only be called by get in cache operation
    @Override
    public boolean checkIfKeyExists(String key) throws IOException {
        rwLock.readLock().lock();
        try {
            boolean found = false;
            byte[] keySizeBytes = new byte[4];
            byte[] valueSizeBytes = new byte[4];

            RandomAccessFile raf = new RandomAccessFile(dataFilePath, "r");
            long currOffset = raf.length();
            if (currOffset == 0) return false;

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

            if (keyString.equals(key) && valueSize != 0) {
                found = true;
            }
            raf.close();
            return found;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public void clearStorage() {
        logger.info("deleting storage data file " + dataFilePath + "...");
        File storageFile = new File(dataFilePath);
        if (storageFile.exists()) {
            storageFile.delete();
            logger.info("storage data file " + dataFilePath + " deleted");
        }
        assert(!storageFile.exists());
        createStorageFile();
    }
}