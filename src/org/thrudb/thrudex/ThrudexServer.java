package org.thrudb.thrudex;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.thrift.TProcessor;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.THsHaServer.Options;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.thrudb.thrudex.lucene.ThrudexLuceneHandler;

public class ThrudexServer {
	
	private String indexRoot;
	private int    port;
	private int    threadCount;
	private Logger logger = Logger.getLogger(this.getClass().getSimpleName());
	
	
	public String getIndexRoot() {
		return indexRoot;
	}

	public void setIndexRoot(String indexRoot) {
		this.indexRoot = indexRoot;
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

	
	public void start() {
		//Start the server
		try {
			logger.info("Starting thrudex service");
			logger.info("index root: " + this.getIndexRoot());
			logger.info("service port: " + this.getPort());
			logger.info("thread count: " + this.getThreadCount());

			//Transport
			TNonblockingServerTransport serverSocket =
				new TNonblockingServerSocket(port);

			//Processor
			TProcessor  processor = 
				new Thrudex.Processor(new ThrudexLuceneHandler(indexRoot)); 

			Options opt = new Options();
			opt.maxWorkerThreads = threadCount;

			//Server
			TServer server = new THsHaServer(processor,serverSocket);
			
			//Serve
			server.serve();

		} catch(Exception e) {
			logger.error("Exception caught:", e);
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
	
		File       propFile   = null;
		Properties properties = new Properties();
		
		//check args
		for (int i=0; i<args.length; i++) {

			if(args[i].equalsIgnoreCase("-f")) {
				if (i+1 >= args.length) {
					printUsageAndExit();
				}
				else {
					propFile = new File(args[i+1]);
					if(propFile.exists() && propFile.canRead() && propFile.isFile()) {
						try {
							properties.load(new FileInputStream(propFile));
						} catch(Exception e) {
							System.err.println("Error while loading properties file: "
										+ args[i+1] + " (" + e.getMessage() + ")");
							System.exit(1);
						}
					} else {
						System.err.println("Invalid properties file: "+args[i+1]);
						System.exit(1);
					}
				}
			}
		}

		if(properties.isEmpty()) {
			printUsageAndExit();
		}
		
		PropertyConfigurator.configure(propFile.getAbsolutePath());
		
		ThrudexServer thrudexServer = new ThrudexServer();
		
		if(!properties.containsKey("INDEX_ROOT")){
			System.err.println("INDEX_ROOT Property Required");
			System.exit(1);
		}

		String indexRoot = properties.getProperty("INDEX_ROOT");
		thrudexServer.setIndexRoot(indexRoot);
		
		int port = Integer.valueOf(properties.getProperty("SERVER_PORT","9090"));
		thrudexServer.setPort(port);

		int threadCount = Integer.valueOf(properties.getProperty("THREAD_COUNT", "5"));
		thrudexServer.setThreadCount(threadCount);

		thrudexServer.start();
	}
	
	private static void printUsageAndExit()
	{
		System.err.println("Usage : thrudex -f thrudex.properties");
		System.exit(1);
	}
}
