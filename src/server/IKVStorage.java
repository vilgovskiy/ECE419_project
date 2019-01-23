package server;

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;

public interface IKVStorage {
    String storagePath = ".//storage//";

    void writeToDisk (String key, String value) throws FileNotFoundException, UnsupportedEncodingException;

    String getFileContents(String key) throws FileNotFoundException;

}
