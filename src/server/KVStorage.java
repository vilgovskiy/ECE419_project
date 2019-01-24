package server;

import org.apache.log4j.Logger;
import java.io.File;
import java.io.RandomAccessFile;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public class KVStorage implements IKVStorage {

    private static Logger logger = Logger.getRootLogger();
    private static final String storageDir = "./data";
    private static final String fileExt = ".db";
    //private static int maxLength = 4096 * 1024;

    private String storageFilePath;
    private File storageFile;
    //private long fileLength;

    public KVStorage(String fileName) {
        this.storageFilePath = storageDir + "/" + fileName + fileExt;
        openStorageFile();
    }

    public void openStorageFile() {
        if (storageFile == null) {
            logger.info("creating storage file...");
            File dir = new File(storageDir);
            if (!dir.exists()) {
                boolean mkdirResult = dir.mkdir();
                if (!mkdirResult) {
                    logger.error("cannot create data directory " + storageDir);
                    return;
                }
            }
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

    @Override
    public void put(String key, String value) throws IOException {
        //TODO
    }

    @Override
    public String get(String key) throws IOException {
        return key;
    }

    @Override
    public long write(KVData data) throws IOException {
        //if (data.keySize + data.valueSize + 8 + fileLength >= maxLength) {
            // raise exception due to insufficient file size
        //    logger.warn("maximum file size reached for " + storageFilePath);
        //}
        RandomAccessFile raf = new RandomAccessFile(storageFilePath, "rw");
        long fileLength = raf.length();
        raf.seek(fileLength);
        raf.write(data.toByteArray());
        raf.close();
        return fileLength;
    }

    @Override
    public KVData read(String key) throws IOException {
        KVData foundEntry = new KVData();
        boolean found = false;
        RandomAccessFile raf = new RandomAccessFile( storageFilePath, "r");
        long fileLength = raf.length();
        long currOffset = 0;

        while (currOffset < fileLength) {
            raf.seek(currOffset);
            byte[] keySizeBytes = new byte[4];
            byte[] valueSizeBytes = new byte[4];
            raf.read(keySizeBytes);
            currOffset += 4;
            raf.read(valueSizeBytes);
            currOffset +=4;

            int keySize = ByteBuffer.wrap(keySizeBytes).getInt();
            byte[] keyBytes = new byte[keySize];
            raf.read(keyBytes);
            String keyString = new String(keyBytes, Charset.forName("UTF-8"));
            currOffset += keySize;
            int valueSize = ByteBuffer.wrap(valueSizeBytes).getInt();

            if (keyString.equals(key)) {
                byte[] valueBytes = new byte[valueSize];
                raf.read(valueBytes);
                String value = new String(valueBytes, Charset.forName("UTF-8"));
                foundEntry.setKey(key);
                foundEntry.setValue(value);
                found = true;
            } else {
                currOffset += valueSize;
            }
        }
        raf.close();
        return found ? foundEntry : null;
    }

    @Override
    public KVData readFromIndex(String key, long index) throws Exception {
        KVData foundEntry = new KVData();
        long offset = index;
        byte[] keySizeBytes = new byte[4];
        byte[] valueSizeBytes = new byte[4];

        RandomAccessFile raf = new RandomAccessFile(storageFilePath, "r");
        raf.seek(offset);
        raf.read(keySizeBytes);
        raf.read(valueSizeBytes);

        int keySize = ByteBuffer.wrap(keySizeBytes).getInt();
        int valueSize = ByteBuffer.wrap(valueSizeBytes).getInt();

        byte[] keyBytes = new byte[keySize];
        raf.read(keyBytes);
        String keyString = new String(keyBytes, Charset.forName("UTF-8"));

        if (keyString.equals(key)) {
            byte[] valueBytes = new byte[valueSize];
            raf.read(valueBytes);
            String value = new String(valueBytes, Charset.forName("UTF-8"));
            foundEntry.setKey(key);
            foundEntry.setValue(value);
            raf.close();
            return foundEntry;
        } else {
            logger.error("cannot find key " + key + " from index");
            raf.close();
            return null;
        }
    }


}
