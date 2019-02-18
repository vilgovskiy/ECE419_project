package ecs;

import org.apache.log4j.Logger;

import javax.xml.bind.DatatypeConverter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class ECSNode implements IECSNode {

    private static Logger logger = Logger.getRootLogger();
    private static MessageDigest messageDigest;
    private String name;
    private String host;
    private Integer port;
    private String prevNodeHash;
    private String hash;

    @Override
    public ServerStatus getStatus() {
        return status;
    }

    @Override
    public void setStatus(ServerStatus status) {
        this.status = status;
    }

    public enum ServerStatus {
        OFFLINE,
        INACTIVE, // ssh launched but not yet communicate with ECS yet
        STOP,
        ACTIVE,
    }

    private ServerStatus status;


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
        this.prevNodeHash = "";

        if(_host != null && _port != null){
            this.hash = ECSNode.calculateHash(_host + ":" + _port);
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

    @Override
    public String getPrevNode(){return prevNodeHash;}

    @Override
    public void setPrev(String _prev){
        this.prevNodeHash = _prev;
    }

    @Override
    public String getNodeHash(){
        return hash;
    }

    public static String calculateHash(String str){
        messageDigest.reset();
        messageDigest.update(str.getBytes());

        String result = DatatypeConverter
                .printHexBinary(messageDigest.digest()).toUpperCase();

        return result;
    }


    @Override
    public String[] getNodeHashRange() {
        if (prevNodeHash.isEmpty()){
            return null;
        }
        return new String[]{this.prevNodeHash, this.getNodeHash()};
    }
}
