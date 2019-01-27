package server.storage;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public class KVData {
    private static final String encoding = "UTF-8";

    int keySize;
    int valueSize;
    private String key;
    private String value;

    public KVData() {
        this.keySize = 0;
        this.valueSize = 0;
        this.key = "";
        this.value = "";
    }

    public KVData(String key, String value) {
        this.key = key;
        this.value = value;
        this.keySize = key.getBytes(Charset.forName(encoding)).length;
        this.valueSize = value.getBytes(Charset.forName(encoding)).length;
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

    // return the unit size of the data entry in byte size
    public int getDataSize() {
        return keySize + valueSize + 8;
    }

    // convert KVData into byte array
    public byte[] toByteArray() {
        // create byte arrays of keySize, valueSize, key, value fields
        byte[] keySizeBytes = ByteBuffer.allocate(4).putInt(keySize).array();
        byte[] valueSizeBytes = ByteBuffer.allocate(4).putInt(valueSize).array();
        byte[] keyBytes = key.getBytes(Charset.forName(encoding));
        byte[] valueBytes = value.getBytes(Charset.forName(encoding));

        // create new byte array with this KVData's unit size
        byte[] byteArray = new byte[this.getDataSize()];
        // copy into byteArray in order of key, value, keySize, valueSize
        System.arraycopy(keyBytes, 0, byteArray, 0, keySize);
        System.arraycopy(valueBytes, 0, byteArray, keySize, valueSize);
        System.arraycopy(keySizeBytes, 0, byteArray, keySize + valueSize, 4);
        System.arraycopy(valueSizeBytes, 0, byteArray, keySize + valueSize + 4, 4);
        return byteArray;
    }

}