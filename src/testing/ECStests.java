package testing;

import com.google.gson.Gson;
import ecs.ECSConsistentHash;
import ecs.ECSNode;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.SortedMap;

public class ECStests extends TestCase {


    @Test
    public void testECSNodeCreation(){
        ECSNode node = new ECSNode("node_name", "localhost", 50000);
    }

    @Test
    public void testECSRingInsert(){

        ECSConsistentHash hashRing = new ECSConsistentHash();
        ECSNode node = new ECSNode("node_name", "localhost", 50000);
        ECSNode node1 = new ECSNode("node_name", "google.com", 50001);
        ECSNode node2 = new ECSNode("node_name", "yandex", 50543);

        hashRing.addNode(node);
        hashRing.addNode(node1);
        hashRing.addNode(node2);
    }

    @Test
    public void testSerializationOfHashRing(){
        ECSConsistentHash hashRing = new ECSConsistentHash();
        ECSNode node = new ECSNode("node_name", "localhost", 50000);
        ECSNode node1 = new ECSNode("node_name", "google.com", 50001);
        ECSNode node2 = new ECSNode("node_name", "yandex", 50543);

        hashRing.addNode(node);
        hashRing.addNode(node1);
        hashRing.addNode(node2);

        String serialized = hashRing.serializeHash();
        Gson gson = new Gson();
        Object result = gson.fromJson(serialized, SortedMap.class);
        SortedMap<String, ECSNode> ringRebuilt = (SortedMap<String, ECSNode>) result;
        assert  ringRebuilt != null;
    }

    @Test
    public void testUpdateMetadata(){
        ECSConsistentHash hashRing = new ECSConsistentHash();
        ECSNode node = new ECSNode("node_name", "localhost", 50000);
        ECSNode node1 = new ECSNode("node_name", "google.com", 50001);
        ECSNode node2 = new ECSNode("node_name", "yandex", 50543);

        hashRing.addNode(node);
        hashRing.addNode(node1);
        hashRing.addNode(node2);

        String serializedRing = hashRing.serializeHash();

        ECSConsistentHash hashRing2 = new ECSConsistentHash();
        hashRing2.updateConsistentHash(serializedRing);
        ECSNode testNew = new ECSNode("test_node", "localhost", 123456);
        hashRing2.addNode(testNew);
        assertTrue(hashRing2.getRingSize() == 4 );
    }

    @Test
    public void testSerializeEmptyHashRing(){
        ECSConsistentHash hashRing = new ECSConsistentHash();
        String serialized = hashRing.serializeHash();
        assertTrue(!serialized.isEmpty());

        ECSConsistentHash hashRing2 = new ECSConsistentHash();
        hashRing2.updateConsistentHash(serialized);
        assert (hashRing.empty());
    }
}
