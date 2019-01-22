package server;

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;

public interface IKVStorage {
    String storagePath = ".//storage//";

    public void writeToDisk (String key, String value) throws FileNotFoundException, UnsupportedEncodingException;

}
