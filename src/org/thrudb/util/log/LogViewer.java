package org.thrudb.util.log;

import java.io.File;

import org.apache.thrift.TException;

import tokyocabinet.HDB;

/**
 * Displays the contents of a log
 * 
 * @author jake
 * 
 */
public class LogViewer {

	private static HDB hdb;
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {

		try {
			if (args.length == 0) {
				System.err.println("usage: LogViewer logfile");
				System.exit(1);
			}

			// verify db file
			File logFile = new File(args[0]);

			if (logFile.isFile() && !logFile.canWrite())
				throw new Exception(args[0] + " is not writable");

			if (logFile.isDirectory())
				throw new Exception(args[0] + " should not be a directory");

			int hdbFlags = HDB.OWRITER;
			hdb = new HDB();
			
			if (!hdb.open(args[0], hdbFlags)) {
				throw new Exception(hdb.errmsg());
			}
			
			printStats();
			
		} catch (Throwable t) {
			System.err.println(t.getLocalizedMessage());
			System.exit(1);
		}
	}
	
	private static void printStats(){
		System.out.println("Total logged records: "+hdb.rnum()/2);
		
		int pos = hdb.addint("LSN",0);
		
		if(pos == Integer.MIN_VALUE)
			System.out.println(hdb.errmsg());
		
		System.out.println("LSN at "+pos);
		for(int i=1; i<pos; i++){
			//get value size;
			String key = String.valueOf(i)+";";
			byte[] val = hdb.get(key.getBytes());
			if(val == null)
				continue;
			
			System.out.println("key="+key+", size="+hdb.vsiz(key)+" bytes, c/e="+new String(hdb.get(key+"r")));
		}
	}

}
