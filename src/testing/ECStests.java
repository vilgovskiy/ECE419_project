package testing;

import ecs.ECSNode;
import junit.framework.TestCase;
import org.junit.Test;

public class ECStests extends TestCase {

    @Test
    public void testECSNodeCreation(){
        ECSNode node = new ECSNode("node_name", "localhost", 50000);
    }

}
