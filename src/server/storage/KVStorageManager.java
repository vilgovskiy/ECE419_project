package server.storage;

import org.apache.log4j.Logger;
import java.io.IOException;
import java.io.File;
import java.util.List;
import java.util.ArrayList;


// Singleton class that manages all the KVStorage instances
public class KVStorageManager {
    private static Logger logger = Logger.getRootLogger();
    private static List<KVStorageInfo> kvStorageInfoList; //keeps track of all the persistent KVStorage files and its length
    private static KVStorageManager instance = null; // singleton instance
    private static KVStorage latestKVStorage = null; // latest file to append to, all other previous files are read-only
    private static String storageDir = ".data"; // storage directory where all the data files will be in
    private static long totalLength = 0; // total length of all current persistent KVStorage files
    static long maxFileLength = 2048 * 1024; // max file length the KVStorage file can have

    // class that contains KVStorage instance and its current file length
    class KVStorageInfo {
        KVStorage kvStorage;
        long fileLength;

        KVStorageInfo(KVStorage kvStorage) {
            this.kvStorage = kvStorage;
            this.fileLength = 0;
        }
    }

    // constructor for KVStorage manager
    private KVStorageManager() {
        // check if the storage directory exists, if not create it
        logger.info("creating storage directory ...");
        File dir = new File(storageDir);
        if (!dir.exists()) {
            if (!dir.mkdir()) {
                logger.error("cannot create storage directory " + storageDir + "! ");
                return;
            }
        }
        // initialize kvstorageinfo list if not yet initialized
        if (kvStorageInfoList == null) {
            kvStorageInfoList = new ArrayList<>();
        }

        File[] files = dir.listFiles();
        // no files found, initialize the first KVStorage instance and all to the info list
        if (files.length == 0 ) {
            createNewKVStorage();
        } else { // if the server crashes, this will load all existing data files, and add to the kvstorageInfo lists
            for (File dataFile : files ) {
                String fileName = storageDir + "/" + dataFile.getName();
                loadExistingKVStorage(fileName, dataFile.length());
            }
            latestKVStorage = kvStorageInfoList.get(kvStorageInfoList.size()-1).kvStorage;
        }
    }

    // Singleton method to get instance
    public static KVStorageManager getInstance() {
        if (instance == null) { instance = new KVStorageManager(); }
        return instance;
    }

    // method that creates a new KVStorage and append to list
    private synchronized void createNewKVStorage() {
        int currIndex = kvStorageInfoList.size();
        KVStorage newStorage = new KVStorage(String.valueOf(currIndex));
        KVStorageInfo storageInfo = new KVStorageInfo(newStorage);
        kvStorageInfoList.add(storageInfo);
        latestKVStorage = newStorage;
    }

    // method that loads existing KVStorage from fileName into kvStorageInfoList
    private void loadExistingKVStorage(String fileName, long fileLength) {
        logger.info("loading existing storage file " + fileName + " ...");
        KVStorage kvStorage = new KVStorage(fileName, fileLength);
        KVStorageInfo storageInfo = new KVStorageInfo(kvStorage);
        kvStorageInfoList.add(storageInfo);
    }

    // put method that writes the new kvData entry to the latest KVStorage
    public synchronized long put(KVData kvData) throws IOException {
        long totalOffsetIndex = totalLength - latestKVStorage.getCurrLength();
        if (latestKVStorage.getCurrLength() + kvData.getDataSize() > maxFileLength) {
            createNewKVStorage();
            totalOffsetIndex = totalLength;
        }
        long latestFileOffset = latestKVStorage.write(kvData);
        return latestFileOffset + totalOffsetIndex;
    }

    // Supposed to create thread pool workers that calls KVStorage reader in KVStorage class file
    // the thread should call the KVStorage.read(key) that returns the foundEntry
    // this method should return the KVData foundEntry from the latest KVStorage file and disregard others
    // for example: if found entry from 1.db, 2.db, 3.db, then it should return the foundEntry matched in 3.db because
    // it is the newest update file
    public KVData get(String key) throws IOException {
        //TODO
        return new KVData();
    }

    // get the value from index if key exists in cache, calls readFromIndex in KVStorage instance
    public KVData getFromIndex(String key, long getIndex) {
        // if
        for (KVStorageInfo storageInfo : kvStorageInfoList) {
            if (getIndex < storageInfo.fileLength) {
                try {
                    return storageInfo.kvStorage.readFromIndex(key, getIndex);
                } catch (IOException e) {
                    logger.error("IO Exception during get " + key + " from index "
                            + Long.valueOf(getIndex));
                    return null;
                }
            }
            else getIndex -= storageInfo.fileLength;
        }
        return null;
    }
}











