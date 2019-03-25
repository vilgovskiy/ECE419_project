package testing;

import ecs.ECSConsistentHash;
import ecs.ECSNode;
import junit.framework.TestCase;
import org.junit.Test;
import server.ReplicationManager;

import java.io.IOException;

public class ReplicationManagerTest extends TestCase {
    ReplicationManager repMan = new ReplicationManager("TestServer", "127.0.0.1", 4000);


    @Test
    public void testUpdateReplicationListOneServer() {

        ECSConsistentHash ring = new ECSConsistentHash();
        ring.addNode(new ECSNode("TestServer", "127.0.0.1", 4000));
        try {
            repMan.updateReplicatorList(ring);
        } catch (AssertionError er) {
            assertTrue(true);
        } catch (IOException e) {
            assertFalse(true);
        }
    }

    @Test
    public void testUpdateReplicationListTwoServers() {

        ECSConsistentHash ring = new ECSConsistentHash();
        ring.addNode(new ECSNode("TestServer", "127.0.0.1", 4000));
        ring.addNode(new ECSNode("TestServer2", "127.0.0.1", 4001));
        try {
            repMan.updateReplicatorList(ring);
        } catch (AssertionError er) {
            assertTrue(true);
        } catch (IOException e) {
            assertFalse(true);
        }
    }



    @Test
    public void testUpdateReplicationList() {

        ECSConsistentHash ring = new ECSConsistentHash();
        ring.addNode(new ECSNode("TestServer", "127.0.0.1", 4000));
        ring.addNode(new ECSNode("TestServer2", "127.0.0.1", 4001));
        ring.addNode(new ECSNode("TestServer3", "127.0.0.1", 4002));
        try {
            repMan.updateReplicatorList(ring);
        } catch (IOException e) {
            assertFalse(true);
        }
    }

    @Test
    public void testUpdateReplicationListWithRemoved() {

        ECSConsistentHash ring = new ECSConsistentHash();
        ring.addNode(new ECSNode("TestServer", "127.0.0.1", 4000));
        ring.addNode(new ECSNode("TestServer2", "127.0.0.1", 4001));
        ring.addNode(new ECSNode("TestServer3", "127.0.0.1", 4002));
        try {
            repMan.updateReplicatorList(ring);
        } catch (IOException e) {
            assertFalse(true);
        }
        ring.removeNode(ring.getNodeByNodeName("TestServer").getNodeHash());

        try {
            repMan.updateReplicatorList(ring);
        } catch (AssertionError er) {
            assertTrue(true);
        } catch (IOException e) {
            assertFalse(true);
        }
    }

    @Test
    public void testReUpdateReplicationList() {

        ECSConsistentHash ring = new ECSConsistentHash();
        ring.addNode(new ECSNode("TestServer", "127.0.0.1", 4000));
        ring.addNode(new ECSNode("TestServer2", "127.0.0.1", 4001));
        ring.addNode(new ECSNode("TestServer3", "127.0.0.1", 4002));

        try {
            repMan.updateReplicatorList(ring);
        } catch (IOException e) {
            assertFalse(true);
        }


        ring.addNode(new ECSNode("TestServer4", "127.0.0.1", 4003));

        try {
            repMan.updateReplicatorList(ring);
        } catch (IOException e) {
            assertFalse(true);
        }
    }

    @Test
    public void testClearReplicationList() {

        ECSConsistentHash ring = new ECSConsistentHash();
        ring.addNode(new ECSNode("TestServer", "127.0.0.1", 4000));
        ring.addNode(new ECSNode("TestServer2", "127.0.0.1", 4001));
        ring.addNode(new ECSNode("TestServer3", "127.0.0.1", 4002));

        try {
            repMan.updateReplicatorList(ring);
        } catch (IOException e) {
            e.printStackTrace();
        }

        repMan.clearReplicatorList();

    }


}
