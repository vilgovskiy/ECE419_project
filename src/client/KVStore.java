package client;

import java.net.Socket;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;

import ecs.ECSConsistentHash;
import ecs.ECSNode;
import shared.communication.AbstractCommunication;
import shared.messages.KVMessage;
import shared.messages.JsonMessage;
import shared.messages.TextMessage;
import server.sql.SQLParser;

import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.Set;


public class KVStore extends AbstractCommunication implements KVCommInterface {

	private Logger logger = Logger.getRootLogger();
	private boolean running;
	private String address;
	private int port;
	private Socket clientSocket;

	private ECSConsistentHash ecsHashRing;

	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		this.address = address;
		this.port = port;
		ECSNode newNode = new ECSNode("server1", this.address, this.port);
		ecsHashRing = new ECSConsistentHash();
		ecsHashRing.addNode(newNode);
	}

	public String getAddress() { return address; }

	public int getPort() { return port; }

	public boolean isRunning(){
		return running;
	}

	public void setRunning(boolean run){ running = run; }

	@Override
	public void connect() throws Exception {
		if (clientSocket != null) {
			logger.info("connection already established");
		}
		else {
			clientSocket = new Socket(address, port);
			input = new BufferedInputStream(clientSocket.getInputStream());
			output = new BufferedOutputStream(clientSocket.getOutputStream());
			setRunning(true);
			logger.info("connection established at " + address + ":" + port);
		}
	}

	@Override
	public void disconnect() {
		logger.info("try to close connection ...");
		try {
			logger.info("tearing down connection ...");
			if (clientSocket != null ){
				clientSocket.close();
				clientSocket = null;
				setRunning(false);
				logger.info("connection closed");
			}
		} catch(IOException ioe){
			logger.error("cannot close connection!!!");
		}
	}

	private void reconnectToCorrectServer(KVMessage req) throws Exception {
		String keyHash;
		if (req.getStatus().equals(KVMessage.StatusType.SQL)) {
			keyHash = ECSNode.calculateHash(SQLParser.getTableFromSQLMsg(req).get(0));
		} else {
			keyHash = ECSNode.calculateHash(req.getKey());
		}

		ECSNode correctServer = ecsHashRing.getNodeByKeyHash(keyHash);
		String correctHost = correctServer.getNodeHost();
		int correctPort = correctServer.getNodePort();

		if (!(this.address.equals(correctHost) && this.port == correctPort)) {
			this.address = correctHost;
			this.port = correctPort;
			logger.info("Now connecting to correct server at " + this.address + ":" + this.port);
			disconnect();
			connect();
		}
	}

	private KVMessage resendForNotResponsibleResp(KVMessage req, KVMessage resp) throws Exception {
		if (resp.getStatus().equals(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE)) {
			String keyHash;
			if (req.getStatus().equals(KVMessage.StatusType.SQL)) {
				keyHash = ECSNode.calculateHash(SQLParser.getTableFromSQLMsg(req).get(0));
				//System.out.println("TABLE IS " + SQLParser.getTableFromSQLMsg(req));
			} else {
				keyHash = ECSNode.calculateHash(req.getKey());
			}

			ecsHashRing = new ECSConsistentHash(resp.getMetadata());
			ECSNode correctServer = ecsHashRing.getNodeByKeyHash(keyHash);

			this.address = correctServer.getNodeHost();
			this.port = correctServer.getNodePort();

			logger.debug("Key hash: " + keyHash);
			logger.info("NotResponsibleResponse, now connecting to host " + this.address + ":" + this.port
						+ " with keyHashRange " + correctServer.getNodeHashRange()[0] +
						" ~ " + correctServer.getNodeHashRange()[1]);

			disconnect();
			connect();

			if (req.getStatus().equals(KVMessage.StatusType.GET)) {
				return this.get(req.getKey());
			} else if (req.getStatus().equals(KVMessage.StatusType.PUT)) {
				return this.put(req.getKey(), req.getValue());
			} else if (req.getStatus().equals(KVMessage.StatusType.SQL)) {
				return this.sql(req.getKey());
			} else {
				logger.fatal("Error, not supposed to happen");
			}
		}
		return resp;
	}


	@Override
	public KVMessage put(String key, String value) throws Exception {
		logger.info("PUT request to server. Key: " + key + " , Value: " + value);

		JsonMessage jsonReq = new JsonMessage(KVMessage.StatusType.PUT, key, value);
		reconnectToCorrectServer(jsonReq);
		TextMessage req = new TextMessage(jsonReq.serialize());
		sendMessage(req);
		TextMessage resp = receiveMessage();
		JsonMessage jsonResp = new JsonMessage();
		jsonResp.deserialize(resp.getMsg());
		return resendForNotResponsibleResp(jsonReq, jsonResp);
	}

	@Override
	public KVMessage get(String key) throws Exception {
		logger.info("GET request to server. Key: " + key);

		JsonMessage jsonReq = new JsonMessage(KVMessage.StatusType.GET, key, "");
		reconnectToCorrectServer(jsonReq);
		TextMessage req = new TextMessage(jsonReq.serialize());
		sendMessage(req);
		TextMessage resp = receiveMessage();
		JsonMessage jsonResp = new JsonMessage();
		jsonResp.deserialize(resp.getMsg());
		return resendForNotResponsibleResp(jsonReq, jsonResp);
	}

	@Override
	public KVMessage sql(String sql) throws Exception {
		logger.info("Executing SQL statement " + sql);
		JsonMessage jsonReq = new JsonMessage(KVMessage.StatusType.SQL, sql, "");
		reconnectToCorrectServer(jsonReq);
		TextMessage req = new TextMessage(jsonReq.serialize());
		sendMessage(req);
		TextMessage resp = receiveMessage();
		JsonMessage jsonResp = new JsonMessage();
		jsonResp.deserialize(resp.getMsg());
		return resendForNotResponsibleResp(jsonReq, jsonResp);
	}
}
