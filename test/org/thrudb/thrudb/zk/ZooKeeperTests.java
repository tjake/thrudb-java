package org.thrudb.thrudb.zk;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

public class ZooKeeperTests extends TestCase implements Watcher {

	Logger logger = Logger.getLogger(getClass());
	ZooKeeper zk;
	String znode = "/znode";
	CountDownLatch connectLatch = new CountDownLatch(1);
	CountDownLatch deleteLatch = new CountDownLatch(1);
	
	public void testClient() {
		try {
			zk = new ZooKeeper("localhost:2182", 3000, this);
			if(!connectLatch.await(2,TimeUnit.SECONDS)) //wait for connection
				fail("Can't connect to zk");
				
			//create a new node
			zk.create(znode, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			
			
			//add some kids
			zk.create(znode+"/foo", null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
			zk.create(znode+"/foo", null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
			zk.create(znode+"/foo", null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
			
			List<String> kids = zk.getChildren(znode, false);
			
			assertEquals(3, kids.size());
			
			//delete kids
			for(String path : kids){
				logger.debug(path);
				zk.delete(znode+"/"+path, -1);
			}
			
			//create watcher on the node
			zk.exists(znode, true);
			
			Thread.sleep(1000);
			
			//delete and wait for watch trigger 
			zk.delete(znode, -1);		
			deleteLatch.await();
			
			//zk.close();
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
	
	
	public void process(WatchedEvent event) {
		
		try {
			String path = event.getPath();
		
			logger.debug(path);
			
			if (event.getType() == Event.EventType.None) {
				// We are are being told that the state of the
				// connection has changed
				logger.debug("connection state change");
				
				switch (event.getState()) {
				case SyncConnected:
					// In this particular example we don't need to do anything
					// here - watches are automatically re-registered with
					// server and any watches triggered while the client was
					// disconnected will be delivered (in order of course)
					
					connectLatch.countDown();
					break;
				case Expired:
					// It's all over
					fail("Connection Expired");
					break;
				}
			} else {
				if (path != null && path.equals(znode)) {
					// Something has changed on the node, let's find out
					deleteLatch.countDown();
				}
			}
		} catch (Throwable t) {
			t.printStackTrace();
			fail();
		}
	}

}
