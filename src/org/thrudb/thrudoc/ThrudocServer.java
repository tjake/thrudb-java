package org.thrudb.thrudoc;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.THsHaServer.Options;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.thrudb.thrift.TPeekingTransportFactory;

public class ThrudocServer {

	private String docRoot;
	private String propertyName;
	
	public String getLogRoot() {
		return propertyName;
	}

	public void setLogRoot(String logRoot) {
		this.propertyName = logRoot;
	}


	private int    port;
	private int    threadCount;
	private TServer server;
	
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
				new ThrudocLoggingProcessor(new ThrudocHandler(docRoot)); 
			
			Options opt = new Options();
			opt.maxWorkerThreads = threadCount;
			
			TPeekingTransportFactory peekFactory = new TPeekingTransportFactory(propertyName,"thrudoc_log");
			
			//Server
			//TServer server = new THsHaServer(processor,serverSocket);
			server = new THsHaServer( new TProcessorFactory(processor), serverSocket,
                    peekFactory, peekFactory,
                    new TBinaryProtocol.Factory(),
                    new TBinaryProtocol.Factory(),
                    opt);
			
			
			//Server
			server.serve();
			
			
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public void stop(){
		server.stop();
	}
	
	public static String checkDirProperty(Properties properties, String propertyName){
		String property = properties.getProperty(propertyName);
		
		if(property == null){
			System.err.println(propertyName+" missing from property file");
			System.exit(0);
		}
		
		File log = new File(property);
		if(!log.exists()){
			if(log.mkdirs()){
				System.out.println("Created dir: "+property);
			}else{
				System.err.println("Failed to create dir:"+property);
				System.exit(0);
			}
		}else if(!log.isDirectory()) {
			System.err.println("exists but not a directory:"+property);
			System.exit(0);
		}else if(!log.canWrite()) {
			System.err.println("dir exists but not writable:"+property);
			System.exit(0);
		}else{
			System.out.println(propertyName+": "+property);
		}
		
		return propertyName;
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
		
		String docRoot = checkDirProperty(properties, "DOC_ROOT");
		thrudocServer.setDocRoot(docRoot);
		
		String logRoot = checkDirProperty(properties, "LOG_ROOT");
		thrudocServer.setLogRoot(logRoot);

		
		int port = Integer.valueOf(properties.getProperty("SERVER_PORT","9090"));
		System.out.println("service port: "+port);	
		thrudocServer.setPort(port);
		
		int threadCount = Integer.valueOf(properties.getProperty("THREAD_COUNT", "5"));
		System.out.println("thread count: "+threadCount);
		thrudocServer.setThreadCount(threadCount);
		
		
		thrudocServer.start();	
	}
}
