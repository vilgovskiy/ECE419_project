package server;

import app_kvServer.IKVServer;
import org.apache.log4j.Logger;


import java.io.*;
import java.net.Socket;
import java.security.MessageDigest;

import ecs.*;
import shared.communication.AbstractCommunication;
import shared.messages.TextMessage;
import shared.messages.JsonMessage;
import shared.messages.KVMessage;

import com.google.gson.JsonSyntaxException;

import java.lang.IllegalStateException;


public class KVClientConnection extends AbstractCommunication implements Runnable {
    private static Logger logger = Logger.getRootLogger();

    private IKVServer kvServer;
    private boolean isOpen;

    private Socket clientSocket;

    private ReplicationManager replicationManager;

    /**
     * Constructs a new CientConnection object for a given TCP socket.
     *
     * @param clientSocket the Socket object for the client connection.
     */
    public KVClientConnection(IKVServer kvServer, Socket clientSocket) {
        this.kvServer = kvServer;
        this.clientSocket = clientSocket;
        this.isOpen = true;
        this.replicationManager = kvServer.getReplicationManager();
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

                    TextMessage respText = new TextMessage(response.serialize());
                    sendMessage(respText);

                    /* connection either terminated by the client or lost due to
                     * network problems*/
                } catch (JsonSyntaxException | IllegalStateException se) {
                    logger.warn("Error parsing incoming message from client! ", se);
                    isOpen = false;
                } catch (IOException ioe) {
                    logger.error("Error! Connection lost!");
                    isOpen = false;
                } catch (Exception e) {
                    logger.error("Error receiving connection!", e);
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
                    clientSocket = null;
                }
            } catch (IOException ioe) {
                logger.error("Error! Unable to tear down connection!", ioe);
            }
        }
    }

    private JsonMessage processMsg(JsonMessage msg) {
        JsonMessage response = new JsonMessage();
        String hashedKey = ECSNode.calculateHash(msg.getKey());
        logger.info("Received message from " + clientSocket.getInetAddress());

        if (kvServer.serverStopped()) {
            logger.info("Server stopped, not processing any client requests!");
            response.setStatus(KVMessage.StatusType.SERVER_STOPPED);
            return response;
        } else if ( kvServer.inServerKeyRange(hashedKey)) {
            String server = kvServer.getHostname() + ":" + kvServer.getPort();
            logger.info(server + " not responsible for key " + msg.getKey());
            response.setStatus(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);

            // Get metadata and send back to client
            ECSConsistentHash metadata = kvServer.getMetadata();
            response.setMetadata(metadata.serializeHashRing());
            return response;
        }

        switch (msg.getStatus()) {
            case REPLICA_PUT:
            case PUT: {
                if (kvServer.writeLocked()) {
                    logger.info("Write requests are currently blocked");
                    response.setStatus(KVMessage.StatusType.SERVER_WRITE_LOCK);
                    break;
                }
                logger.info("PUT request for {\"key\": " + msg.getKey() + ", \"value\": " + msg.getValue() + "}");
                response = kvServer.putKV(msg.getKey(), msg.getValue());

                try {
                    if (msg.getStatus().equals(KVMessage.StatusType.PUT)) {
                        replicationManager.sendReplicaPuts(msg);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                break;
            }
            case GET:
                logger.info("GET request for {\"key\": " + msg.getKey() + ", \"value\": " + msg.getValue() + "}");
                response = kvServer.getKV(msg.getKey());
                break;
            default:
        }
        return response;
    }
}
