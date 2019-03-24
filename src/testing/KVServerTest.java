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
        server.start();

        String host = "127.0.0.1";
        int port = 5000;
        store = new KVStore(host, port);

        node = new ECSNode("testServer", host, port);
        ECSNode node2 = new ECSNode("testServer1", host, 5001);
        metadata = new ECSConsistentHash();
        metadata.addNode(node);
        metadata.addNode(node2);
    }

    public void tearDown() {
        server.clearStorage();
        server.close();
    }

    @Test
    public void testUpdateMetadata() {
        String json = metadata.serializeHashRing();
        server.updateMetadata(json);
        String[] range = server.getRange();

        String start = "E73EB7EDC6B16F4BFDBFE7BD78F9AC14";
        String end = "73909F8C96A9D08E876411C0A212A1F4";

        assertEquals(start, range[0]);
        assertEquals(end, range[1]);

    }

    @Test
    public void testCheckRange() {
        String json = metadata.serializeHashRing();
        server.updateMetadata(json);
        String hashedKey = "F3909F8C96A9D08E876411C0A212A1F4";
        assertTrue(server.inServerKeyRange(hashedKey));
    }

    @Test
    public void testServerStopped() {
        try {
            store.connect();
            server.rejectClientRequests();

            // Upon start, server should be stopped
            KVMessage msg = store.put("key", "value");
            assertEquals(KVMessage.StatusType.SERVER_STOPPED, msg.getStatus());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            store.disconnect();
        }
    }

    @Test
    public void testServeWriteLocked() {
        server.lockWrite();

        try {
            store.connect();

            KVMessage msg = store.put("key", "value");
            assertEquals(msg.getStatus(), KVMessage.StatusType.SERVER_WRITE_LOCK);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            store.disconnect();
        }
    }

    @Test
    public void testServerWriteUnlock() {
        server.lockWrite();

        try {
            store.connect();
            server.unlockWrite();
            KVMessage msg = store.put("key", "value");
            assertNotSame(msg.getStatus(), KVMessage.StatusType.SERVER_WRITE_LOCK);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            store.disconnect();
        }
    }

    @Test
    public void testServerResponsible() {
        String json = metadata.serializeHashRing();
        server.updateMetadata(json);

        // server is responsible for range
        // e73eb7edc6b16f4bfdbfe7bd78f9ac14 (hash of 127.0.0.1:5000) to
        // 73909f8c96a9d08e876411c0a212a1f4 (hash of 127.0.0.1:5001)
        try {
            store.connect();

            // hash of key is 3c6e0b8a9c15224a8228b9a98ca1531d
            KVMessage msg = store.put("key", "value");
            assertEquals(KVMessage.StatusType.PUT_SUCCESS, msg.getStatus());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            store.disconnect();
        }
    }
}
