package app_kvClient;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import logger.LogSetup;

import java.io.IOException;
import java.net.UnknownHostException;

import client.KVCommInterface;
import client.KVStore;
import shared.messages.KVMessage;



public class KVClient implements IKVClient, Runnable {

    private static Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "Client> ";
    private BufferedReader stdin;
    private KVStore store = null;
    private boolean stop = false;
    
    private String serverAddr;
    private int serverPort;

    @Override
    public void run(){
        while(!stop) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);

            try {
                String cmdLine = stdin.readLine();
                this.handleCommand(cmdLine);
            } catch (IOException ioe){
                stop = true;
                printError("CLI does not respond - Application terminated ");
            }
        }
    }

    private void handleCommand(String cmdLine) {
        String[] token = cmdLine.split("\\s+");
        String cmd = token[0];

        switch (cmd) {
            case "connect":
                if (token.length == 3) {
                    try {
                        serverAddr = token[1];
                        serverPort = Integer.parseInt(token[2]);
                        newConnection(serverAddr, serverPort);
                    } catch(NumberFormatException nfe) {
                        printError("No valid address. Port must be a number!");
                        logger.info("Unable to parse argument <port>", nfe);
                    } catch (UnknownHostException e) {
                        printError("Unknown Host!");
                        logger.info("Unknown Host!", e);
                    } catch (IOException ioe) {
                        printError("Could not establish connection!");
                        logger.warn("Could not establish connection!", ioe);
                    } catch (Exception e) {
                        printError("Unknown error!");
                        logger.warn("Unknown error!", e);
                    }
                    
                } else {
                    printError("Invalid number of parameters!");
                }
                break;

            case "disconnect":
                closeConnection();
                break;

            case "get":
                if (token.length == 2) {
                    if (store != null && store.isRunning()){
                        try {
                            store.get(token[1]);
                        } catch (Exception e) {
                            printError("get failed!");
                        }
                    }
                } else {
                    printError("Invalid number of parameters!");
                }
                break;
            
            case "put":
                if (token.length == 3) {
                    if (store != null && store.isRunning()){
                        try {
                            store.put(token[1], token[2]);
                        } catch (Exception e) {
                            printError("put failed!");
                        }
                    }
                } else {
                    printError("Invalid number of parameters!");
                }
                break;

            case "logLevel":
                if(token.length == 2) {
			    	String level = setLevel(token[1]);
			    	if(level.equals(LogSetup.UNKNOWN_LEVEL)) {
			    		printError("No valid log level!");
			    		printPossibleLogLevels();
			    	} else {
			    		System.out.println(PROMPT + 
			    				"Log level changed to level " + level);
			    	}
			    } else {
			    	printError("Invalid number of parameters!");
			    }
                break;
            
            case "help":
                printHelp();
                break;

            case "quit":
                stop = true;
                closeConnection();
                System.out.println(PROMPT + "Application exit!");
                break;
            
            default: 
                printError("Unknown command");
                printHelp();
        }
    }

    @Override
    public void newConnection(String hostname, int port) throws Exception {
        if (store != null) {
            logger.warn("connection has already been established at " + store.getAddress() + ":" + store.getPort());
        }
        else {
            logger.info("connecting to server at " + hostname + ":" + (port));
            store = new KVStore(hostname, port);
            store.connect();
        }
    }

    private void closeConnection() {
        if (store != null) {
            store.disconnect();
            store = null;
        }
    }

    @Override
    public KVCommInterface getStore(){
        return store;
    }
    
    private void printHelp() {
		StringBuilder sb = new StringBuilder();
		sb.append(PROMPT).append("KV CLIENT HELP (Usage):\n");
		sb.append(PROMPT);
		sb.append("::::::::::::::::::::::::::::::::");
		sb.append("::::::::::::::::::::::::::::::::\n");
		sb.append(PROMPT).append("connect <host> <port>");
		sb.append("\t establishes a connection to a server\n");
		sb.append(PROMPT).append("get <key>");
		sb.append("\t\t Retrieves the value for the given key from the storage server\n");
        sb.append(PROMPT).append("put <key> <value>");
		sb.append("\t\t\t inserts/updates a key-value pair into the storage server, if no value ");
        sb.append("is provided, the entry is deleted\n");
        sb.append(PROMPT).append("disconnect");
		sb.append("\t\t\t disconnects from the server \n");
		
		sb.append(PROMPT).append("logLevel");
		sb.append("\t\t\t changes the logLevel \n");
		sb.append(PROMPT).append("\t\t\t\t ");
		sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");
		
		sb.append(PROMPT).append("quit ");
		sb.append("\t\t\t exits the program");
		System.out.println(sb.toString());
	}

    private void printPossibleLogLevels() {
		System.out.println(PROMPT 
				+ "Possible log levels are:");
		System.out.println(PROMPT 
				+ "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
	}

    private String setLevel(String levelString) {
		if(levelString.equals(Level.ALL.toString())) {
			logger.setLevel(Level.ALL);
			return Level.ALL.toString();
		} else if(levelString.equals(Level.DEBUG.toString())) {
			logger.setLevel(Level.DEBUG);
			return Level.DEBUG.toString();
		} else if(levelString.equals(Level.INFO.toString())) {
			logger.setLevel(Level.INFO);
			return Level.INFO.toString();
		} else if(levelString.equals(Level.WARN.toString())) {
			logger.setLevel(Level.WARN);
			return Level.WARN.toString();
		} else if(levelString.equals(Level.ERROR.toString())) {
			logger.setLevel(Level.ERROR);
			return Level.ERROR.toString();
		} else if(levelString.equals(Level.FATAL.toString())) {
			logger.setLevel(Level.FATAL);
			return Level.FATAL.toString();
		} else if(levelString.equals(Level.OFF.toString())) {
			logger.setLevel(Level.OFF);
			return Level.OFF.toString();
		} else {
			return LogSetup.UNKNOWN_LEVEL;
		}
	}

    private void printError(String error){
		System.out.println(PROMPT + "Error! " +  error);
    }

}
