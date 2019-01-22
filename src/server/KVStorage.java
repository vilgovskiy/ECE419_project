package server;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

public class KVStorage implements IKVStorage {

    public KVStorage(){
        if (!(new File(storagePath).exists())){
            new File(storagePath).mkdir();
        }
    }

    @Override
    public void writeToDisk(String key, String value) throws FileNotFoundException, UnsupportedEncodingException {
        PrintWriter writer = new PrintWriter(storagePath + key + ".txt", "UTF-8");
        writer.write(value);
        writer.close();
    }

    public String findFile(String key){
        File kv = new File(storagePath + key + ".txt");
        String value = kv.toString();
        return value;
    }


}
