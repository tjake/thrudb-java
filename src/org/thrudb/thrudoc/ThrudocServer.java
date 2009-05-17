package org.thrudb.thrudoc;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;
import org.apache.lucene.store.FSDirectory;
import org.apache.thrift.TProcessor;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.THsHaServer.Options;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.thrudb.thrudoc.tokyocabinet.ThrudocHandler;

public class ThrudocServer {

	private String docRoot;
	private int    port;
	private int    threadCount;
	
	
	public String getDocRoot() {
		return docRoot;
	}

	public void setDocRoot(String docRoot) {
		this.docRoot = docRoot;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public int getThreadCount() {
		return threadCount;
	}

	public void setThreadCount(int threadCount) {
		this.threadCount = threadCount;
	}

	
	public void start(){
		//Start the server
		try{
			//Transport
			TNonblockingServerTransport serverSocket =
				new TNonblockingServerSocket(port);
			
			
			//Processor
			TProcessor  processor = 
				new Thrudoc.Processor(new ThrudocHandler(docRoot)); 
			
			Options opt = new Options();
			opt.maxWorkerThreads = threadCount;
			
			//Server
			TServer server = new THsHaServer(processor,serverSocket);
			
			//Serve
			server.serve();
			
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
	
		File       propFile   = null;
		Properties properties = new Properties();
		
		//check args
		for(int i=0; i<args.length; i++){
			
			if(args[i].equalsIgnoreCase("-f")){
				if(i+1 >= args.length){
				
					System.out.println("Thrudoc [-h] [-f thrudoc.properties]");
					System.exit(-1);
				
				}else{
					propFile = new File(args[i+1]);
					if(propFile.exists() && propFile.canRead() && propFile.isFile()){
						try{
							properties.load(new FileInputStream(propFile));
						}catch(Exception e){
							System.out.println("invalid properties file: "+args[i+1]);
							System.exit(0);
						}
					} else {
						System.out.println("invalid properties file: "+args[i+1]);
						System.exit(0);
					}
				}
				
			}
		}		
		
		if(properties.isEmpty()){
			System.out.println("Thrudoc[-h] [-f thrudoc.properties]");
			System.exit(-1);
		}
		
		PropertyConfigurator.configure(propFile.getAbsolutePath());
		
		ThrudocServer thrudocServer = new ThrudocServer();
		
		if(!properties.containsKey("DOC_ROOT")){
			System.out.println("DOC_ROOT Property Required");
			System.exit(0);
		}
		
		//make sure root exits
		String docRoot = properties.getProperty("DOC_ROOT");
		File doc = new File(docRoot);
		if(!doc.exists()){
			if(doc.mkdirs()){
				System.out.println("Created doc root: "+docRoot);
			}else{
				System.err.println("Failed to create doc root:"+docRoot);
				System.exit(0);
			}
		}else if(!doc.isDirectory()) {
			System.err.println("doc root exists but not a directory:"+docRoot);
			System.exit(0);
		}else if(!doc.canWrite()) {
			System.err.println("doc root exists but not writable:"+docRoot);
			System.exit(0);
		}else{
			System.out.println("doc root: "+docRoot);
		}
		
		thrudocServer.setDocRoot(docRoot);
		
		
		int port = Integer.valueOf(properties.getProperty("SERVER_PORT","9090"));
		System.out.println("service port: "+port);	
		thrudocServer.setPort(port);
		
		int threadCount = Integer.valueOf(properties.getProperty("THREAD_COUNT", "5"));
		System.out.println("thread count: "+threadCount);
		thrudocServer.setThreadCount(threadCount);
		
		
		thrudocServer.start();	
	}
}
