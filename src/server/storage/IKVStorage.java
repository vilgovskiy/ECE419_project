package server.storage;

import java.util.Map;

public interface IKVStorage {

    void write(KVData kvData) throws Exception;

    KVData read(String key) throws Exception;

    boolean checkIfKeyExists(String key) throws Exception;

    void clearStorage();

	public Map<String, String> getAllKVData() throws Exception;

}
