package server;

import org.apache.log4j.Logger;

import ecs.*;
import shared.communication.AbstractCommunication;
import shared.messages.*;

import java.net.Socket;
import java.io.IOException;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;

public class ServerReplicator extends AbstractCommunication {
    private Logger logger = Logger.getRootLogger();

    private String nodeHost = "127.0.0.1";
    private int nodePort;
    private Socket socket;
//    private BufferedInputStream input;
//    private BufferedOutputStream output;

    public ServerReplicator(IECSNode node) {
        nodeHost = node.getNodeHost();
        nodePort = node.getNodePort();
    }

    // connect to designated replica node
    public void connect() {
        try {
            this.socket = new Socket(nodeHost, nodePort);
            this.input = new BufferedInputStream(socket.getInputStream());
            this.output = new BufferedOutputStream(socket.getOutputStream());
        } catch (IOException e) {
            logger.warn("Cannot initialize Server replicator for " + nodeHost + ":" + nodePort);
        }
    }

    // send REPLICA_PUT message to designated replica node
    public void replicaPut(KVMessage message) throws Exception {
        JsonMessage jsonReq = new JsonMessage();
        jsonReq.setKey(message.getKey());
        jsonReq.setValue(message.getValue());
        KVMessage.StatusType status = message.getStatus();
        if (status.equals(KVMessage.StatusType.PUT)) {
            jsonReq.setStatus(KVMessage.StatusType.REPLICA_PUT);
        }

        TextMessage msg = new TextMessage(jsonReq.serialize());
        sendMessage(msg);

        JsonMessage jsonResp = new JsonMessage();
        jsonResp.deserialize(receiveMessage().getMsg());

        if (jsonResp.getStatus().equals(KVMessage.StatusType.PUT_SUCCESS) ||
                jsonResp.getStatus().equals(KVMessage.StatusType.DELETE_SUCCESS) ||
                jsonResp.getStatus().equals(KVMessage.StatusType.PUT_UPDATE)) {
            logger.debug("Replicate PUT Success for " + this.nodeHost +":"+ this.nodePort);
        } else {
            throw new Exception("Replicate PUT failed for " + this.nodeHost + ":" + this.nodePort);
        }
    }

    // close connection to replica node
    public void close() {
        try {
            if (this.socket != null) {
                this.output.close();
                this.input.close();
                socket.close();
            }
        } catch (IOException e) {
            logger.error("Error occured when closing connection for ServerReplicator.");
        }
    }

    public String getNodeHost() { return nodeHost; }
    public int getNodePort() { return nodePort; }
}
