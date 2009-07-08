package org.thrudb.thrudoc;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.ZooKeeperServerMXBean;
import org.apache.zookeeper.server.NIOServerCnxn.Factory;

public class ThrudocTests extends TestCase {

	private ExecutorService zkServiceThread = Executors
			.newSingleThreadExecutor();
	private ExecutorService serviceThread = Executors.newSingleThreadExecutor();
	private Thrudoc.Client client;
	private TFramedTransport transport;
	private static Logger logger = Logger.getLogger(ThrudocTests.class);

	public void setUp() {
		try {
			zkServiceThread.submit(new ZkThread());
			Thread.sleep(1000);
			
			serviceThread.submit(new ThrudocTestService());
			Thread.sleep(1000);

			//client
			TSocket socket = new TSocket("localhost", 11291);
			transport = new TFramedTransport(socket);
			TBinaryProtocol protocol = new TBinaryProtocol(transport);

			client = new Thrudoc.Client(protocol);

			transport.open();

		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	public void tearDown() {
		transport.close();
		serviceThread.shutdownNow();
		zkServiceThread.shutdownNow();

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			fail(e.getMessage());
		}

	}

	public class ZkThread extends Thread {

		int port;
		ZooKeeperServer server;
		File dataDir = new File(".", "zkdata");
		File logDir  = new File(".", "zklogs");
		
		Factory nioFactory;

		public ZkThread() throws IOException {
			port = 2182;
	
			if (!dataDir.exists() && !dataDir.mkdir()) 
				throw new IOException("unable to mkdir " + dataDir);
			
			if (!logDir.exists() && !logDir.mkdir()) 
				throw new IOException("unable to mkdir " + logDir);			
		}

		public void run() {

			try {
				
				server = new ZooKeeperServer(dataDir, logDir, 2000);
				
				nioFactory = new NIOServerCnxn.Factory(port);
				nioFactory.startup(server);
				
			} catch (final IOException e) {
				throw new RuntimeException(
						"Unable to start single ZooKeeper server.", e);
			} catch (final InterruptedException e) {
				throw new RuntimeException("ZooKeeper server was interrupted.",
						e);
			}

		}

		public void shutdown() {
			nioFactory.shutdown();
		}
	}

	class ThrudocTestService implements Runnable {
		private ThrudocServer thrudocServer = new ThrudocServer();

		public void run() {
			try {

				thrudocServer.setDocRoot(".");
				thrudocServer.setLogRoot(".");
				thrudocServer.setPort(11291);
				thrudocServer.setThreadCount(5);

				thrudocServer.start();
				logger.info("thrudoc started");

			} catch (Throwable t) {
				thrudocServer.stop();
				fail(t.toString());
			}
		}
	}

	public void testUUID() {
		byte[] uuid = UUID.randomUUID().toString().getBytes();
		String bucket = "thrudb";

		try {
			// List<String> keys = client.scan(bucket, "", 10);

			client.create_bucket(bucket);
			client.put(bucket, ThrudocBackend.SPECIAL_KEY, uuid);

			byte[] t = client.get(bucket, ThrudocBackend.SPECIAL_KEY);

			assertTrue(uuid.length == t.length);

		} catch (Exception e) {
			fail(e.getLocalizedMessage());
		}

	}

	public void testMapOperations() {
		String bucket = "test_bucket";

		try {
			client.delete_bucket(bucket);
			client.create_bucket(bucket);

			byte[] value = client.get(bucket, "key");
			if (value.length > 0)
				fail("key should not exist");

			client.put(bucket, "key", "value".getBytes());

			assertTrue("value".equals(new String(client.get(bucket, "key"))));

		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		}
	}

}
