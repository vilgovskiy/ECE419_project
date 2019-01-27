package server;

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;

public interface IKVStorage {
    String storagePath = ".//storage//";
    String txtExtension = ".txt";

    void writeToDisk (String key, String value) throws FileNotFoundException, UnsupportedEncodingException;

    String getFileContents(String key) throws FileNotFoundException;

    boolean deleteFile(String key);

    boolean checkIfFileExists(String key);

	void clearStorage ();
}
