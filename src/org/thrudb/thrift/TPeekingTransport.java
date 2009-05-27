package org.thrudb.thrift;

import org.apache.log4j.Logger;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import tokyocabinet.HDB;

/**
 * Allows a way to peek at the content of the message and rewind back to start.
 * 
 * This is used to log certain actions to disk for redo-logging
 * 
 * @author jake
 * 
 */
public class TPeekingTransport extends TFramedTransport {
	
	private Logger  logger      = Logger.getLogger(getClass());
	private byte[]  peekBuffer  = new byte[] {};
	private byte[]  writeBuffer = new byte[] {}; 
	private int     writeMax    = 1024; //only store the initial output message
	private int     writePos    = 0;
	
	private int     replayPos  = 0;
	private boolean replayMode = false;
	private int     replayEnd  = 0; //stop reply at this point in the buf
	private boolean recording  = false;
	
	private boolean logging    = false;
	private HDB     log;
	private int     nextLogId  = -1;
	

	public TPeekingTransport(TTransport baseTransport, HDB log) {
		super(baseTransport);
		this.log = log;
	}

	@Override
	public void close() {
		super.close();
	}

	@Override
	public boolean isOpen() {
		return super.isOpen();
	}

	public boolean isRecording() {
		return recording;
	}

	public void setRecording(boolean recording) {
		this.recording = recording;
	}

	@Override
	public void open() throws TTransportException {
		super.open();
	}

	@Override
	public int read(byte[] buf, int off, int len) throws TTransportException {
		if (replayMode && replayPos + len <= replayEnd ) {
			
			System.arraycopy(peekBuffer, replayPos, buf, 0, len);
			
			replayPos += len;
			
			return len;
			
		}else{
			
			int sz = super.read(buf, off, len);
			
			// Add to peek buffer
			if (recording && sz > 0) {
				byte[] newPeekBuffer = new byte[peekBuffer.length + sz];
				
				System.arraycopy(peekBuffer, 0, newPeekBuffer, 0, peekBuffer.length);
				System.arraycopy(buf, 0, newPeekBuffer, peekBuffer.length, sz);
				
				peekBuffer = newPeekBuffer;							
			}
			
			return sz;
		}
		
	}

	@Override
	public void write(byte[] buf, int off, int len) throws TTransportException {
		
		if(writeBuffer.length + len < writeMax){
			byte[] newWriteBuffer = new byte[writeBuffer.length + len];
		
			System.arraycopy(writeBuffer, 0, newWriteBuffer, 0, writeBuffer.length);
			System.arraycopy(buf, 0, newWriteBuffer, writeBuffer.length, len);
		
			writeBuffer = newWriteBuffer;			
		}
		
		super.write(buf, off, len);
	}

	public boolean isReplayMode() {
		return replayMode;
	}

	public void setReplayMode(boolean replayMode) {
		this.replayMode = replayMode;
		
		if(replayMode == true)
			replayEnd = peekBuffer.length;
	}
	
	
	/**
	 * This is a hack for reading the output struct type
	 * 
	 */
	public void swapInWriteBuffer(){
		peekBuffer = writeBuffer;
		replayPos  = 0;
		replayMode = true;
		recording  = false;
		replayEnd  = writeMax;
	}
	
	public void reset(){
		peekBuffer  = new byte[] {};
		replayPos  = 0;
		replayMode = false;
	    recording  = false;
	    
	    writeBuffer = new byte[] {};
		writePos    = 0;
	
	}
	
	public byte[] getBuffer(){
		return peekBuffer;
	}
	
	public boolean isLogging() {
		return logging;
	}

	public void setLogging(boolean logging) {
		this.logging = logging;
		
		//get new log in sequence
		if(logging){
			nextLogId = log.addint("LSN", 1);
		}
	
	}
}


