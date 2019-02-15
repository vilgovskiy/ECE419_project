package ecs;

import org.apache.log4j.Logger;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class ECSNode implements IECSNode {

    private static Logger logger = Logger.getRootLogger();
    private static MessageDigest messageDigest;
    private String name;
    private String host;
    private Integer port;
    private ECSNode prev;
    private String hash;

    static {
        try {
            messageDigest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            messageDigest = null;
            logger.error("Hashing algorithm failed to be initialized");
        }

    }
    public ECSNode(String _name, String _host, Integer _port){
        this.name = _name;
        this.host = _host;
        this.port = _port;

        if(_host != null && _port != null){
            this.hash = calculateHash(_host + ":" + _port);
        }

    }

    @Override
    public String getNodeName() {

        return name;
    }

    @Override
    public String getNodeHost() {
        return host;
    }

    @Override
    public int getNodePort() {
        return port;
    }

    public String getNodeHash(){
        return hash;
    }

    public String calculateHash(String str){
        messageDigest.reset();
        messageDigest.update(str.getBytes());
        BigInteger result = new BigInteger(1, messageDigest.digest());

        return result.toString(16);
    }


    @Override
    public String[] getNodeHashRange() {
        if (prev == null){
            return null;
        }
        return new String[]{this.prev.getNodeHash(), this.getNodeHash()};
    }
}
