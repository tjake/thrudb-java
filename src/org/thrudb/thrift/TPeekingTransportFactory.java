package org.thrudb.thrift;

import java.io.File;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransport;

import tokyocabinet.HDB;

public class TPeekingTransportFactory extends TFramedTransport.Factory {
	private HDB hdb;

	public TPeekingTransportFactory(String logDir, String logName)
			throws TException {
		int hdbFlags = HDB.OWRITER;

		// verify db file
		String logFileName = logDir + File.separatorChar + logName + ".tch";
		File logFile = new File(logFileName);

		if (logFile.isFile() && !logFile.canWrite())
			throw new TException(logFileName + " is not writable");

		if (logFile.isDirectory())
			throw new TException(logFileName + " should not be a directory");

		if (!logFile.exists())
			hdbFlags |= HDB.OCREAT;

		hdb = new HDB();
		if (!hdb.open(logFileName, hdbFlags)) {
			throw new TException(hdb.errmsg());
		}

	}

	@Override
	public TTransport getTransport(TTransport trans) {
		return new TPeekingTransport(trans, hdb);
	}
}