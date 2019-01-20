package client;

import shared.messages.KVMessage;

import org.apache.log4j.Logger;

public class KVStore implements KVCommInterface {

	private Logger logger = Logger.getRootLogger();
	private boolean running;
	private String address;
	private int port;

	private Socket clientSocket;
	private OutputStream output;
	private InputStream input;

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
		if (clientSocket) {
			logger.info("connection already established");
		}
		else {
			clientSocket = Socket(address, port);
			setRunning(true);
			logger.info("Connection Established");	
		}
	}

	@Override
	public void disconnect() {
		logger.info("try to close connection ...");
		try {
			logger.info("tearing down connection ...");
			if (clientSocket){
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
		return null;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		return null;
	}

	public boolean isRunning(){
		return running;
	}
	
	public void setRunning(boolean run){
		running = run;
	}
	
}
