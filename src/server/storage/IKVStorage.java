package server.storage;

public interface IKVStorage {

    long write(KVData kvData) throws Exception;

    KVData read(String key) throws Exception;

    KVData readFromIndex(String key, long index) throws Exception;

    void clearStorage();

}
