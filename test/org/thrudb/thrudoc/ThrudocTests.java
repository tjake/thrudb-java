package org.thrudb.thrudoc;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import junit.framework.TestCase;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;

public class ThrudocTests extends TestCase {
	
	private ExecutorService serviceThread = Executors.newSingleThreadExecutor();
	private Thrudoc.Client client; 
	private TFramedTransport transport;
	
	
	public void setUp() {
		serviceThread.submit(new ThrudocTestService());
		
		try{
			Thread.sleep(1000);
		}catch(InterruptedException e){
			fail(e.getMessage());
		}
		
		TSocket          socket    = new TSocket("localhost", 11291 );
        transport = new TFramedTransport(socket);
        TBinaryProtocol  protocol  = new TBinaryProtocol(transport);

		client = new Thrudoc.Client(protocol);
		
		try{
			transport.open();
		}catch(TException e){
			fail(e.getMessage());
		}	
	}
	
	public void tearDown(){
		transport.close();
		serviceThread.shutdown();
	}
	
	class ThrudocTestService implements Runnable {
		private ThrudocServer thrudocServer = new ThrudocServer();
		
		
		public void run() {
			try{
				
				thrudocServer.setDocRoot(".");
				thrudocServer.setPort(11291);
				thrudocServer.setThreadCount(5);
				thrudocServer.start();
			}catch(Throwable t){
				thrudocServer.stop();
				fail(t.toString());
			}	
		}	
	}
	
	
	public void testMapOperations(){
		String bucket = "test_bucket";
		
		try{
			client.delete_bucket(bucket);
			client.create_bucket(bucket);
			
			try{
				client.get(bucket, "key");
				fail("key should not exist");
			}catch(InvalidKeyException e){}
			
			client.put(bucket, "key", "value".getBytes());
			
			assertTrue("value".equals(new String(client.get(bucket, "key"))));
			
		}catch(Throwable t){
			t.printStackTrace();
			fail(t.getMessage());
		}
	}
	
	
}
