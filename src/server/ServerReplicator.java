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

    private String nodeHost;
    private int nodePort;
    private Socket socket;
    private BufferedInputStream input;
    private BufferedOutputStream output;

    public ServerReplicator(IECSNode node) {
        nodeHost = node.getNodeName();
        nodePort = node.getNodePort();
    }

    public void replicatePut(KVMessage message) throws Exception {
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
            return;
        } else {
            throw new Exception("Replicate PUT failed for " + this.nodeHost + ":" + this.nodePort);
        }
    }

    public void connect() {
        try {
            this.socket = new Socket(nodeHost, nodePort);
            this.input = new BufferedInputStream(socket.getInputStream());
            this.output = new BufferedOutputStream(socket.getOutputStream());
        } catch (IOException e) {
            logger.warn("Cannot initialize Server replicator for " + nodeHost + ":" + nodePort);
        }
    }

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

}
