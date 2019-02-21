package testing;

import org.junit.Test;

import junit.framework.TestCase;
import app_kvServer.*;
import client.KVStore;
import ecs.*;
import shared.messages.KVMessage;

public class KVServerTest extends TestCase {

    private KVServer server;
    private ECSNode node;
    private ECSConsistentHash metadata;
    private KVStore store;

    public void setUp() {
        server = new KVServer(5000, 20, "FIFO");
        String host = server.getHostname();
        int port = server.getPort();

        store = new KVStore(host, port);

        node = new ECSNode("testServer", host, port);
        ECSNode node2 = new ECSNode("testServer1", host, 5001);
        metadata = new ECSConsistentHash();
        metadata.addNode(node);
        metadata.addNode(node2);
    }

    @Test
    public void testUpdateMetadata() {
        String json = metadata.serializeHash();
        server.updateMetadata(json);
        String[] range = server.getRange();

        String start = "73909f8c96a9d08e876411c0a212a1f4";
        String end = "e73eb7edc6b16f4bfdbfe7bd78f9ac14";

        assertEquals(range[0], start);
        assertEquals(range[1], end);

    }

    @Test
    public void testCheckRange() {
        String json = metadata.serializeHash();
        server.updateMetadata(json);
        String hashedKey = "93909f8c96a9d08e876411c0a212a1f4";
        assertTrue(server.inServerKeyRange(hashedKey));
    }

    @Test
    public void testServerStopped() {
        server.start();

        try {
            store.connect();

            // Upon start, server should be stopped
            KVMessage msg = store.put("key", "value");
            assertEquals(msg.getStatus(), KVMessage.StatusType.SERVER_STOPPED);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testServeWriteLocked() {
        server.start();
        server.lockWrite();

        try {
            store.connect();

            KVMessage msg = store.put("key", "value");
            assertEquals(msg.getStatus(), KVMessage.StatusType.SERVER_WRITE_LOCK);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testServerWriteUnlock() {
        server.start();
        server.lockWrite();

        try {
            store.connect();
            server.unlockWrite();
            KVMessage msg = store.put("key", "value");
            assertNotSame(msg.getStatus(), KVMessage.StatusType.SERVER_WRITE_LOCK);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testServerNotResponsible() {
        String json = metadata.serializeHash();
        server.updateMetadata(json);
        server.start();

        // server is responsible for range
        // 73909f8c96a9d08e876411c0a212a1f4 (hash of 127.0.0.1:5000) to
        // e73eb7edc6b16f4bfdbfe7bd78f9ac14 (hash of 127.0.0.1:5001)
        try {
            store.connect();

            // hash of key is 3c6e0b8a9c15224a8228b9a98ca1531d
            KVMessage msg = store.put("key", "value");
            assertEquals(msg.getStatus(), KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
