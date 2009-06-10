package org.thrudb.thrift;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

public final class ZooKeeperMonitor implements Runnable, Watcher {

	private Logger logger = Logger.getLogger(getClass());
	private ZooKeeper zkServer;
	private String zkAddress;

	private AtomicBoolean connected = new AtomicBoolean(false);
	private CountDownLatch connectLatch = new CountDownLatch(1);
	private final String serverListNode = "serverList";
	private List<String> serverList     = new ArrayList<String>();
	
	public ZooKeeperMonitor(String zkAddress) throws IOException {

		this.zkAddress = zkAddress;
	}

	public List<String> getServerList(){
		return new ArrayList<String>(serverList);
	}
	
	public void run() {
		while (true) {

			try {
				// first connect
				if (!connected.get()) {
					connectLatch = new CountDownLatch(1);
					zkServer = new ZooKeeper(zkAddress, 3000, this);

					connectLatch.await(); // connected

					// setup monitors
					// try{
					// zkServer.create(serverListNode, null,
					// Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					zkServer.create(serverListNode, null, Ids.OPEN_ACL_UNSAFE,
							CreateMode.EPHEMERAL_SEQUENTIAL);
					
					serverList = zkServer.getChildren(serverListNode, true);
					// }catch(NodeExistsException e){
					// this is ok...
					// }
				}

				Thread.sleep(200);

			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			} catch (IOException e) {
				logger.error(e.getMessage());
			} catch (KeeperException e) {
				logger.error(e.getMessage());
			}
		}
	}

	public void process(WatchedEvent event) {

		String path = event.getPath();

		logger.debug(path);

		if (event.getType() == Event.EventType.None) {
			// the state of the connection has changed
			logger.debug("connection state change");

			switch (event.getState()) {

			case SyncConnected:
				connected.set(true);
				connectLatch.countDown();
				break;

			case Expired:
			case Disconnected:
				connected.set(false);
				break;
			}
		} else {
			try {
				if (path != null && path.equals(serverListNode)) {
					serverList = zkServer.getChildren(serverListNode, true);
				}
			} catch (Exception e) {
				logger.error(e.getMessage());
				throw new RuntimeException(e.getMessage());
			}
		}

	}
}
