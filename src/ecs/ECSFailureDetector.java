package ecs;

import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import server.ServerMetadata;

public class ECSFailureDetector implements Watcher {
    private static Logger logger = Logger.getRootLogger();
    private ECS ecs;
    private IECSNode node;
    private ServerMetadata serverMetadata;


    public ECSFailureDetector(ECS ecs, IECSNode node, ServerMetadata serverMetadata) {
        this.ecs = ecs;
        this.node = node;
        this.serverMetadata = serverMetadata;
    }

    @Override
    public synchronized void process(WatchedEvent e) {
        switch (e.getType()) {
            case NodeDeleted:
                // if node is deleted, process has crashed
                logger.info("Crash detected on node: " + node.getNodeName());
                ecs.recoverFromFailure(node, serverMetadata);
                break;
            default:
                logger.error("Unexpected event detected by failure detector: " +
                             e.getType() + "for node: " + node.getNodeName());
        }
    }
}
