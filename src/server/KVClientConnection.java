package server;

import app_kvServer.IKVServer;
import org.apache.log4j.Logger;


import java.io.*;
import java.net.Socket;
import java.security.MessageDigest;
import java.util.List;
import java.util.Arrays;
import java.util.Set;

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
        logger.info("Received message from " + clientSocket.getInetAddress());

        JsonMessage response = new JsonMessage();
        response.setKey(msg.getKey());

        // if KVserver is stopped then it cannot handle any requests
        if (kvServer.serverStopped()) {
            logger.info("Server " + kvServer.getServerName() + " stopped, not processing any client requests!");
            response.setStatus(KVMessage.StatusType.SERVER_STOPPED);
            return response;
        }

        // if KVserver is not responsible for the key PUT and replica operations (GET, REPLICA_PUT) then
        // respond SERVER_NOT_RESPONSIBLE
        if (!checkIfResponsible(msg)) {
            // set response to NOT_RESPONSIBLE, value to hashRing info
            logger.info("Server " + kvServer.getServerName() + " not responsible for key "
                    + msg.getKey() + " with keyHash" + ECSNode.calculateHash(msg.getKey()));
            response.setStatus(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
            response.setMetadata(kvServer.getHashRingMetadata().serializeHashRing());
            logger.debug(kvServer.getHashRingMetadata().serializeHashRingPretty());
            return response;
        }

        switch (msg.getStatus()) {
            case REPLICA_PUT:
            case PUT: {
                // if KVServer is write locked, cant handle requests
                if (kvServer.writeLocked()) {
                    logger.info("Server " + kvServer.getServerName() + " Write requests are currently blocked");
                    response.setStatus(KVMessage.StatusType.SERVER_WRITE_LOCK);
                    return response;
                }

                logger.info("Server " + kvServer.getServerName() +
                        " PUT request for {\"key\": " + msg.getKey() + ", \"value\": " + msg.getValue() + "}");

                try {
                    response = kvServer.putKV(msg.getKey(), msg.getValue());
                    // if the status is PUT, then it is the coordinator. Send REPLICA_PUT to other replica nodes
                    if (msg.getStatus().equals(KVMessage.StatusType.PUT)) {
                        logger.info("Server " + kvServer.getServerName() + " is coordinator for PUT " +
                                msg.getKey());
                        replicationManager.sendReplicaPuts(msg);
                    }
                } catch (Exception e) {
                    logger.error("Server " + kvServer.getServerName() + " failed at operation " + msg.getStatus() +
                            " for key " + msg.getKey());
                    e.printStackTrace();
                    break;
                }
                break;
            }
            case GET:
                logger.info("Server " + kvServer.getServerName() +
                        " GET request for {\"key\": " + msg.getKey() + ", \"value\": " + msg.getValue() + "}");
                response = kvServer.getKV(msg.getKey());
                break;
            default:
        }
        return response;
    }

    // Check if this KVServer is responsible for key, or any replica operations (GET, REPLICA_PUT) for the key
    private boolean checkIfResponsible(KVMessage msg) {
        String keyHash = ECSNode.calculateHash(msg.getKey());
        ECSConsistentHash hashRingMetadata = kvServer.getHashRingMetadata();

        // check if server is responsible (PUT) for the key
        ECSNode responsibleNode = hashRingMetadata.getNodeByKeyHash(keyHash);
        boolean isResponsible = kvServer.getServerName().equals(responsibleNode.getNodeName());

        // if server is not responsible, check if its responsible for replica operations (GET, REPLICA_PUT)
        List<KVMessage.StatusType> replicaOperations = Arrays.asList(
                KVMessage.StatusType.GET,
                KVMessage.StatusType.REPLICA_PUT
        );

        // if server's name is in replicaNodes name then it is responsible for replica operations
        if (replicaOperations.contains(msg.getStatus())) {
            Set<IECSNode> replicaNodes = hashRingMetadata.getReplicaNodesByCoordinator(responsibleNode);
            for (IECSNode replicaNode : replicaNodes) {
                if (replicaNode.getNodeName().equals(kvServer.getServerName())) isResponsible = true;
                logger.info("Server " + kvServer.getServerName() + " responsible for " + msg.getStatus() +
                        " for key " + msg.getKey());
            }
        }
        return isResponsible;
    }
}
