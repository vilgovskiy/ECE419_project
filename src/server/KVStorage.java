package server;

import java.io.*;

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

    public String getFileContents(String key) throws FileNotFoundException {
        File file = new File(storagePath + key + ".txt");
        BufferedReader br = new BufferedReader(new FileReader(file));
        String st;
        String value = "";
        try {
            while ((st = br.readLine()) != null)
                value = value + st;
        } catch (IOException e) {
            //Do nothing here for now
        }
        return value;
    }

    public boolean deleteFile(String key){
        File file = new File(storagePath + key + txtExtension);
        return file.delete();
    }

    public boolean checkIfFileExists(String key){
        File file = new File(storagePath + key + txtExtension);
        return file.exists();
    }

}
