package org.thrudb.thrudoc;

import java.util.Map;

import org.apache.log4j.Logger;
import org.thrudb.thrudoc.tokyocabinet.TokyoCabinetDB;

public class DBShutdownHandler extends Thread {

	private Map<String,ThrudocBackend> bucketMap;
	private Logger logger = Logger.getLogger(getClass());
	
	public DBShutdownHandler(Map<String,ThrudocBackend> dbIndex) {
		this.bucketMap = dbIndex;
	}

	@Override
	public void run() {

		for(Map.Entry<String,ThrudocBackend> db : bucketMap.entrySet()){
			
			if(db.getValue() == null)
				continue;
			
			logger.info("Shutting down db: " + db.getKey());
			
			db.getValue().shutdown();
		}
		
	}
}
