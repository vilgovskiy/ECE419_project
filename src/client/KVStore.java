package client;

import java.net.Socket;

import shared.communication.AbstractCommunication;
import shared.messages.KVMessage;
import shared.messages.JsonMessage;

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

	@Override
	public void connect() throws Exception {
		if (clientSocket != null) {
			logger.info("connection already established");
		}
		else {
			clientSocket = new Socket(address, port);
			setRunning(true);
			logger.info("Connection Established");	
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

	// TODO: implement request using send/receivemessage
	public void request(KVMessage reqMessage) {

	}


	@Override
	public KVMessage put(String key, String value) throws Exception {
		return new JsonMessage(KVMessage.StatusType.PUT, key, value);
	}

	@Override
	public KVMessage get(String key) throws Exception {
		return new JsonMessage(KVMessage.StatusType.GET, key, "");
	}

	public boolean isRunning(){
		return running;
	}
	
	public void setRunning(boolean run){
		running = run;
	}
	
}
