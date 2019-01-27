package client;

import java.net.Socket;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;

import shared.communication.AbstractCommunication;
import shared.messages.KVMessage;
import shared.messages.JsonMessage;
import shared.messages.TextMessage;

import org.apache.log4j.Logger;

import java.io.IOException;

public class KVStore extends AbstractCommunication implements KVCommInterface {

	private Logger logger = Logger.getRootLogger();
	private boolean running;
	private String address;
	private int port;

	private Socket clientSocket;

	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		this.address = address;
		this.port = port;
	}

	public String getAddress() { return address; }

	public int getPort() { return port; }

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
			logger.info("connection established");
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


	@Override
	public KVMessage put(String key, String value) throws Exception {
		logger.info("PUT request to server. Key: " + key + " , Value: " + value);

		JsonMessage jsonReq = new JsonMessage(KVMessage.StatusType.PUT, key, value);
		TextMessage req = new TextMessage(jsonReq.serialize());
		sendMessage(req);
		TextMessage resp = receiveMessage();
		JsonMessage jsonResp = new JsonMessage();
		jsonResp.deserialize(resp.getMsg());

		return jsonResp;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		logger.info("GET request to server. Key: " + key);

		JsonMessage jsonReq = new JsonMessage(KVMessage.StatusType.GET, key, "");
		TextMessage req = new TextMessage(jsonReq.serialize());
		sendMessage(req);
		TextMessage resp = receiveMessage();
		JsonMessage jsonResp = new JsonMessage();
		jsonResp.deserialize(resp.getMsg());

		return jsonResp;
	}

	public boolean isRunning(){
		return running;
	}

	public void setRunning(boolean run){
		running = run;
	}

}