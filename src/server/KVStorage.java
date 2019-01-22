package server;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

public class KVStorage implements IKVStorage {
    @Override
    public void writeToDisk(String key, String value) throws FileNotFoundException, UnsupportedEncodingException {
        PrintWriter writer = new PrintWriter(storagePath + key + ".txt", "UTF-8");
        writer.write(value);
    }


}
