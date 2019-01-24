package server;

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;

public interface IKVStorage {

    void put(String key, String value) throws Exception;

    String get(String key) throws Exception;

    long write(KVData kvData) throws Exception;

    KVData read(String key) throws Exception;

    KVData readFromIndex(String key, long index) throws Exception;

}
