package server;

import app_kvServer.IKVServer;
import com.google.gson.Gson;
import org.apache.log4j.Logger;


import java.io.*;
import java.net.Socket;

import shared.communication.AbstractCommunication;
import shared.messages.KVMessage;
import shared.messages.TextMessage;
import shared.messages.JsonMessage;

public class KVClientConnection extends AbstractCommunication implements Runnable {
    private static Logger logger = Logger.getRootLogger();

    private IKVServer kvServer;
    private boolean isOpen;
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;

    private Socket clientSocket;

    /**
     * Constructs a new CientConnection object for a given TCP socket.
     *
     * @param clientSocket the Socket object for the client connection.
     */
    public KVClientConnection(IKVServer kvServer, Socket clientSocket) {
        this.kvServer = kvServer;
        this.clientSocket = clientSocket;
        this.isOpen = true;
    }

    @Override
    public void run() {
        try {
            output = new BufferedOutputStream(clientSocket.getOutputStream());
            input = new BufferedInputStream(clientSocket.getInputStream());

            while (isOpen) {
                try {
                    TextMessage text = receiveMessage();
                    JsonMessage msg = new JsonMessage();
                    msg.deserialize(text.getMsg());
                    JsonMessage response = processMsg(msg);


                    /* connection either terminated by the client or lost due to
                     * network problems*/
                } catch (IOException ioe) {
                    logger.error("Error! Connection lost!");
                    isOpen = false;
                }
            }

        } catch (IOException ioe) {
            logger.error("Error! Connection could not be established!", ioe);

        } finally {

            try {
                if (clientSocket != null) {
                    input.close();
                    output.close();
                    clientSocket.close();
                }
            } catch (IOException ioe) {
                logger.error("Error! Unable to tear down connection!", ioe);
            }
        }
    }

    private JsonMessage processMsg(JsonMessage msg) {
        JsonMessage response = new JsonMessage();
        logger.info("Received message from " + clientSocket.getInetAddress());
        switch (msg.getStatus()) {
            case PUT:
                try {
                    logger.info("Put request for key:" + msg.getKey() + " value:" + msg.getValue());
                    kvServer.putKV(msg.getKey(), msg.getValue());
                } catch (Exception e){
                    //TODO Add proper handling for this exception
                }
                break;
            case GET:
                try {
                    logger.info("Put request for key:" + msg.getKey() + " value:" + msg.getValue());
                    kvServer.getKV(msg.getKey());
                } catch (Exception e){
                    //TODO Add proper handling for this exception
                }
                break;
            default:
        }
        return response;
    }
}
