package org.thrudb.thrudex;

import org.apache.thrift.TProcessor;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.thrudb.thrudex.Thrudex;
import org.thrudb.thrudex.lucene.ThrudexLuceneHandler;

public class ThrudexServer {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
	
		
		
		
		
		//Start the server
		try{
			//Transport
			TNonblockingServerTransport serverSocket =
				new TNonblockingServerSocket(9093);
			
			//Processor
			TProcessor  processor = 
				new Thrudex.Processor(new ThrudexLuceneHandler()); 
			
			//Server
			TServer server = new THsHaServer(processor,serverSocket);
			
			//Serve
			server.serve();
			
		}catch(Exception e){
			e.printStackTrace();
		}
		
	}

}
