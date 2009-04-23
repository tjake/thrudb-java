package org.thrudb.thrudex.lucene;

import java.util.Map;

import org.apache.log4j.Logger;

public class IndexShutdownHandler extends Thread {

	private Map<String, LuceneIndex> indexes;
	private Logger logger = Logger.getLogger(getClass());
	
	public IndexShutdownHandler(Map<String, LuceneIndex> indexes){
		this.indexes = indexes;
	}
	
	@Override
	public void run() {

		for(Map.Entry<String,LuceneIndex> index : indexes.entrySet()){
			logger.info("Shutting down index: " + index.getKey());
			index.getValue().shutdown();
		}
		
		
	}

	
}
