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

}
