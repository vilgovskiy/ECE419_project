package server.storage;

public interface IKVStorage {

    void write(KVData kvData) throws Exception;

    KVData read(String key) throws Exception;

    boolean checkIfKeyExists(String key) throws Exception;

    void clearStorage();

}
