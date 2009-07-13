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
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.NIOServerCnxn.Factory;

public class ThrudocTests extends TestCase {
    private final static String bucket = "test_bucket";
    private final static String key    = UUID.randomUUID().toString();
    private static Logger logger = Logger.getLogger(ThrudocTests.class);

    private Thrudoc.Client getClient(int port) throws TTransportException {

        TSocket socket = new TSocket("localhost", 11291);
        TTransport transport = new TFramedTransport(socket);
        TBinaryProtocol protocol = new TBinaryProtocol(transport);

        Thrudoc.Client client = new Thrudoc.Client(protocol);

        transport.open();

        return client;
    }

    public ExecutorService startService(Thread svc) {
        ExecutorService thread = Executors.newSingleThreadExecutor();

        thread.submit(svc);

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            // whatever
        }

        return thread;
    }

    public class ZkService extends Thread {

        int port;
        ZooKeeperServer server;
        String prefix;
        File dataDir;
        File logDir;

        Factory nioFactory;

        public ZkService(String prefix, int port) throws IOException {
            this.port = port;
            this.prefix = prefix;

            dataDir = new File(".", "zkdata_" + prefix);
            logDir = new File(".", "zklogs_" + prefix);

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
                throw new RuntimeException("Unable to start single ZooKeeper server.", e);
            } catch (final InterruptedException e) {
                throw new RuntimeException("ZooKeeper server was interrupted.", e);
            }

        }

        public void shutdown() {
            nioFactory.shutdown();
        }
    }

    class ThrudocTestService extends Thread {
        ThrudocServer thrudocServer;
        int port;
        String prefix;
        File dataDir;
        File logDir;

        public ThrudocTestService(String prefix, int port) throws IOException {
            this.prefix = prefix;
            this.port = port;

            dataDir = new File(".", "doc_" + prefix);
            logDir = new File(".", "log_" + prefix);

            if (!dataDir.exists() && !dataDir.mkdir())
                throw new IOException("unable to mkdir " + dataDir);

            if (!logDir.exists() && !logDir.mkdir())
                throw new IOException("unable to mkdir " + logDir);

            try {
                thrudocServer = new ThrudocServer();
                thrudocServer.setDocRoot(dataDir.getAbsolutePath());
                thrudocServer.setLogRoot(logDir.getAbsolutePath());
                thrudocServer.setPort(port);
                thrudocServer.setThreadCount(5);
            } catch (Throwable t) {
                fail(t.toString());
            }
        }

        public void run() {
            try {

                thrudocServer.start();
                logger.info("thrudoc started");

            } catch (Throwable t) {
                thrudocServer.stop();
                fail(t.toString());
            }
        }

        public void shutdown() {
            thrudocServer.stop();
        }
    }

    public void testSingleServer() {

        try {
            ExecutorService zkThread = startService(new ZkService("zk", 2182));
            ExecutorService tdThread = startService(new ThrudocTestService("td", 11291));

            Thrudoc.Client client = getClient(11291);

            this.checkCreateBucket(client);
            this.checkMap(client);
            // this.doReads(client);

            client.delete_bucket(bucket);
            tdThread.shutdown();
            zkThread.shutdown();

        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getLocalizedMessage());
        }

    }
    
    
    public void testTwoServers() {

        try {
            ExecutorService zkThread = startService(new ZkService("zk", 2182));
            ExecutorService td1Thread = startService(new ThrudocTestService("td", 11291));
            ExecutorService td2Thread = startService(new ThrudocTestService("td2", 11292));

            Thrudoc.Client client1 = getClient(11291);
            Thrudoc.Client client2 = getClient(11292);
            
            client1.create_bucket(bucket);
            client1.setReplicationFactor(bucket,2); 
            
            this.doWrites(client1);

            this.doReads(client1);

            this.doReads(client2);
          
            
            client1.delete_bucket(bucket);
            td1Thread.shutdown();
            td2Thread.shutdown();
            zkThread.shutdown();

        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getLocalizedMessage());
        }
    }

    private void checkCreateBucket(Thrudoc.Client client) {
        try {
            for (int i = 0; i < 100; i++) {
                client.delete_bucket(bucket);
                client.create_bucket(bucket);
            }
        } catch (Throwable t) {
            t.printStackTrace();
            fail(t.getLocalizedMessage());
        }
    }

    private void checkMap(Thrudoc.Client client) {

        try {
            client.delete_bucket(bucket);
            client.create_bucket(bucket);

            byte[] value = client.get(bucket, "key1");
            if (value.length > 0)
                fail("key should not exist");

            for (int i = 0; i < 1000; i++) {
                client.put(bucket, "key"+i, "value".getBytes());

                assertTrue("value".equals(new String(client.get(bucket, "key"+i))));
            }
        } catch (Throwable t) {
            t.printStackTrace();
            fail(t.getMessage());
        }
    }

    private void doWrites(Thrudoc.Client client){
       try{
           for (int i = 0; i < 1000; i++) {
               client.put(bucket, key+i, "value".getBytes());
           }
       }catch(Throwable t){
           t.printStackTrace();
           fail(t.getMessage());
       }
    }
    
    private void doReads(Thrudoc.Client client){
        try{   
            for (int i = 0; i < 1000; i++) {
                assertTrue("value".equals(new String(client.get(bucket, key+i))));
            }
        }catch(Throwable t){
            t.printStackTrace();
            fail(t.getMessage());
        }
    }
    
}
