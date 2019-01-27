package shared;

import app_kvClient.IKVClient;
import app_kvClient.KVClient;
import app_kvServer.IKVServer;
import app_kvServer.KVServer;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import logger.LogSetup;
import java.io.IOException;

public final class ObjectFactory {
	/*
	 * Creates a KVClient object for auto-testing purposes
	 */

	static{
		try {
            new LogSetup("logs/client.log", Level.ALL);
		} catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
		}

	}
    public static IKVClient createKVClientObject() {
        // TODO Auto-generated method stub
    	IKVClient kvClient = new KVClient();
		return kvClient;
    }
    
    /*
     * Creates a KVServer object for auto-testing purposes
     */
	public static IKVServer createKVServerObject(int port, int cacheSize, String strategy) {
		// TODO Auto-generated method stub
		IKVServer kvServer = new KVServer(port, cacheSize, strategy);
		return kvServer;
	}
}
