package org.thrudb.thrift;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.thrudb.thrudoc.ThrudocBackend;
import org.thrudb.thrudoc.ThrudocException;
import org.thrudb.thrudoc.ThrudocHandler;

public final class ZooKeeperMonitor implements Runnable, Watcher {

	private Logger logger = Logger.getLogger(getClass());
	private ZooKeeper zkServer;
	private String zkAddress;
	private ThrudocHandler thrudocHandler;

	private AtomicBoolean connected = new AtomicBoolean(false);
	private CountDownLatch connectLatch = new CountDownLatch(1);
	
	/** node name for server list */
	private final String serverListNode = "/serverList";
	
	/** Keeps track of the instances connected */
	private List<String> serverList; 
	
	/** this instances unique id */
	private String instanceId;
	
	/** node name for space list */
	private final String spaceMapNode = "/spaceMap";
	
	/** Keeps track of the spaces available on each instance */
	private Map<String, List<String>> spaceMap;
	
	public ZooKeeperMonitor(String zkAddress, ThrudocHandler thrudocHandler) {
		this.zkAddress      = zkAddress;
		this.thrudocHandler = thrudocHandler;
		
		try{
			this.instanceId = new String(thrudocHandler.get("thrudb", ThrudocBackend.SPECIAL_KEY));
		}catch(Exception e){
			throw new RuntimeException("Problem identifying this thrudb instance on zookeeper... bailing");
		}
		
		this.serverList = new ArrayList<String>();
		this.spaceMap   = new ConcurrentHashMap<String, List<String>>(); 
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
					try{
						zkServer.create(serverListNode, null, 
								Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
					}catch(KeeperException.NodeExistsException e){
						//this is fine.
					}
					
					zkServer.create(serverListNode+"/id", instanceId.getBytes(), 
							Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
					
					serverList = zkServer.getChildren(serverListNode, true);		
				}

				Thread.sleep(200);

			} catch (InterruptedException e) {
				logger.info("Interrupted");
				Thread.currentThread().interrupt();
				return;
			} catch (IOException e) {
				logger.error("IOException: "+e.getMessage());
			} catch (KeeperException e) {
				logger.error("KeeperException: "+e.getMessage());
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
