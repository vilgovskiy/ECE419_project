package server;

import org.apache.log4j.Logger;

import ecs.*;
import shared.communication.AbstractCommunication;
import shared.messages.*;

import java.net.Socket;
import java.io.IOException;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;

public class KVTransfer extends AbstractCommunication {
    private Logger logger = Logger.getRootLogger();

    private String address;
    private int port;
    private Socket socket;
    private BufferedInputStream input;
    private BufferedOutputStream output;

    public KVTransfer(String address, int port) {
        this.address = address;
        this.port = port;
    }

    // connect to designated replica node
    public void connect() {
        try {
            this.socket = new Socket(address, port);
            this.input = new BufferedInputStream(socket.getInputStream());
            this.output = new BufferedOutputStream(socket.getOutputStream());
        } catch (IOException e) {
            logger.warn("Cannot initialize Transfer of data to " + address + ":" + port);
        }
    }

    // send put message to the server to issue data transfer
    public void put(String key, String value) throws Exception {
        JsonMessage jsonReq = new JsonMessage(KVMessage.StatusType.TRANSFER, key, value);
        TextMessage msg = new TextMessage(jsonReq.serialize());
        sendMessage(msg);

        JsonMessage jsonResp = new JsonMessage();
        jsonResp.deserialize(receiveMessage().getMsg());

        if (jsonResp.getStatus().equals(KVMessage.StatusType.PUT_SUCCESS) ||
                jsonResp.getStatus().equals(KVMessage.StatusType.DELETE_SUCCESS) ||
                jsonResp.getStatus().equals(KVMessage.StatusType.PUT_UPDATE)) {
            logger.debug("Transfer success for " + address +":" + port);
        } else {
            throw new Exception("Transfer failed for " + address + ":" + port);
        }
    }

    // close connection to node
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
