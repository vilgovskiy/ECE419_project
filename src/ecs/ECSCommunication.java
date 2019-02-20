package ecs;

import shared.messages.KVAdminMessage;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static ecs.ECS.ZK_TIMEOUT;

public class ECSCommunication implements Watcher {
	private static Logger logger = Logger.getRootLogger();
	private Zookeeper zk;
	private CountDownLatch latch;

	private Collection<ECSNode> nodes;
	private static long msgNum = 0;

	public ECSCommunication(Zookeeper zk, Collection<ECSNode> nodes)  {
		this.zk = zk;
		this.nodes = nodes;
		this.latch = new CountDownLatch(nodes.size());
	}

	/**
	 * Broadcast KVAdmin message to all the nodes in nodes. This function
	 * returns once the message has been received by all nodes
	 * @param msg the KVAdmin message to send
	 */
	public void broadcast(KVAdminMessage msg) {
		for (ECSNode n : nodes) {
			String msgPath = ECS.getNodePath(n) + "/message" + msgNum;
			msgNum++;

			try {
				zk.create(msgPath, msg.serialize().getBytes(),
							ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

				// set watcher for this node
				Stat exists = zk.exists(msgPath, this);
				if (exists == null) {
					// message has been consumed by a KVServer
					latch.countDown();
				}
			} catch (KeeperException e) {
				logger.error("Could not create message node in path " + msgPath
							+ " : " + e.getMessage());
			}
		}

		// wait until message has been received by all KVServer nodes
		latch.await(ZK_TIMEOUT, TimeUnit.MILLISECONDS);
	}

	@Override
	public void process(WatchedEvent event) {
		// watch has been triggered
		latch.countDown();

		switch (event.getType()) {
			case NodeDeleted:
				// message successfully consumed by KVServer
				break;
			case NodeDataChanged:
				// expected message node to be deleted,
				// but instead node value changed, log error
				String error;
				try {
					error = new String(zk.getData(event.getPath(), false, null));
					logger.error(error);
				} catch (KeeperException | InterruptedException e) {
					logger.error("Error processing message, but error message "
								+ "could not be received");
				}

				zk.delete(event.getPath(), zk.exists(event.getPath(), false).getVersion());
				break;
			default:
				logger.error("Unexpected type received: " + event.getType()
                        	+ " from node " + event.getPath());
				break;
		}
	}
}
