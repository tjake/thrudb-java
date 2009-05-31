package org.thrudb.thrudoc;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.thrudb.thrift.TPeekingTransport;
import org.thrudb.thrudoc.Thrudoc.Iface;
import org.thrudb.thrudoc.Thrudoc.Processor;

import tokyocabinet.HDB;

public class ThrudocLoggingProcessor extends Processor {

	private Set<String> writeOps;
	private Logger logger = Logger.getLogger(getClass());

	
	
	public ThrudocLoggingProcessor(Iface iface) {
		super(iface);
		
		writeOps = new HashSet<String>();
		writeOps.addAll(Arrays.asList(new String[]{
				"create_bucket","delete_bucket","put",
				"push_front","push_back","pop_front","pop_back",
				"erase_at","insert_at","replace_at","incr","decr"
		}));
	}
	
	@Override
	public boolean process(TProtocol iprot, TProtocol oprot) throws TException {
		
		TPeekingTransport peekTrans = (TPeekingTransport) iprot.getTransport();
		TPeekingTransport writeTrans = (TPeekingTransport) oprot.getTransport();
		
		//Just peek at the initial message
		peekTrans.setRecording(true);
		
		TMessage msg = iprot.readMessageBegin();
		
		peekTrans.setRecording(false);
		
		if(writeOps.contains(msg.name)){
			logger.info("logging "+msg.name);

			peekTrans.setLogging(true);
		}
		
		peekTrans.setReplayMode(true);
		
		
		boolean result = super.process(iprot, oprot);
	
		//only log operations that alter the db
		if(writeOps.contains(msg.name)){
			writeTrans.swapInWriteBuffer();
			
			msg = oprot.readMessageBegin();
			
			//dont log operations that caused an exceptions
			if(msg.type == TMessageType.EXCEPTION){
				
				//roll back message
				peekTrans.rollback();
			} else {
				peekTrans.commit();
			}
			
		}
		
		
		peekTrans.reset();
		
		
		
		return result;
	
	}
	
	private void writeLog(byte[] logMessage){
		
	}
	

}
