package server;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public class KVData {
    static final String encoding = "UTF-8";

    long offset;
    int keySize;
    int valueSize;
    String key;
    String value;

    public KVData() {}

    public KVData(String key, String value) {
        this.key = key;
        this.value = value;
        this.keySize = key.getBytes().length;
        this.valueSize = value.getBytes().length;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public void setKey(String key) {
        this.key = key;
        this.keySize = key.getBytes().length;
    }

    public void setValue(String value) {
        this.value = value;
        this.valueSize = value.getBytes().length;
    }

    public int getSize() {
        return keySize + valueSize + 8;
    }

    // convert KVData into byte array
    public byte[] toByteArray() {
        byte[] keySizeBytes = ByteBuffer.allocate(4).putInt(keySize).array();
        byte[] valueSizeBytes = ByteBuffer.allocate(4).putInt(keySize).array();
        byte[] keyBytes = key.getBytes(Charset.forName("UTF-8"));
        byte[] valueBytes = value.getBytes(Charset.forName("UTF-8"));

        byte[] byteArray = new byte[8 + keyBytes.length + valueBytes.length];
        System.arraycopy(keySizeBytes, 0, byteArray, 0, 4);
        System.arraycopy(valueSizeBytes, 0, byteArray, 4, 4);
        System.arraycopy(keyBytes, 0, byteArray, 8, keyBytes.length);
        System.arraycopy(valueBytes, 0, byteArray, 8 + keyBytes.length, valueBytes.length);

        return byteArray;
    }

}