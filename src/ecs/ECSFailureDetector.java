package ecs;

import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class ECSFailureDetector implements Watcher {
    private static Logger logger = Logger.getRootLogger();
    private ECS ecs;
    private IECSNode node;

    public ECSFailureDetector(ECS ecs, IECSNode node) {
        this.ecs = ecs;
        this.node = node;
    }

    @Override
    public synchronized void process(WatchedEvent e) {
        switch (e.getType()) {
            case NodeDeleted:
                // if node is deleted, process has crashed
                logger.info("Crash detected on node: " + node.getNodeName());

                // TODO: crash recovery function
                break;
            default:
                logger.error("Unexpected event detected by failure detector: " +
                             e.getType() + "for node: " + node.getNodeName());
        }
    }
}
