package org.thrudb.thrift;

import java.io.File;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import tokyocabinet.HDB;


/**
 * This should use thread local storage... till commit.
 * @author jake
 *
 */
public class TPeekingTransportFactory extends TFramedTransport.Factory implements LogManager {
    private HDB log;

    private static Logger logger = Logger.getLogger(TPeekingTransportFactory.class);

    public TPeekingTransportFactory(String logDir, String logName) throws TException {
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

        log = new HDB();
        if (!log.open(logFileName, hdbFlags)) {
            throw new TException(log.errmsg());
        }

    }

    @Override
    public TTransport getTransport(TTransport trans) {
        return new TPeekingTransport(trans, this);
    }

    
    //Log routines
    public String getCurrentLSN(String bucket) throws TTransportException {
        int lsn = log.addint("LSN_" + bucket, 0);
        if (lsn == Integer.MIN_VALUE) {
            throw new TTransportException("Logging error:" + log.errmsg());
        } else {
            logger.info("LSN is currently " + lsn + " for " + bucket);
        }

        return String.valueOf(lsn);
    }
    
    public String getNextLSN(String bucket) throws TTransportException {
        int lsn = log.addint("LSN_" + bucket, 1);
        if (lsn == Integer.MIN_VALUE) {
            throw new TTransportException("Logging error:" + log.errmsg());
        } else {
            logger.info("LSN is now " + lsn + " for " + bucket);
        }

        return String.valueOf(lsn);
    }
    
    public void log(String bucket, String lsn, byte[] buf) throws TTransportException {
        if(!log.putcat((bucket+"_"+lsn).getBytes(), buf)){
            throw new TTransportException("Log message"+lsn+" is corrupt");
        }
    }
    
    public void commit(String bucket,String lsn) throws TTransportException {
        if(!log.put(bucket+"_"+lsn+"_r", "c")){
            throw new TTransportException("Logging commit err:"+log.errmsg());
        }
        log.sync();
    }
    
    public void rollback(String bucket, String lsn) throws TTransportException {
        if(!log.put(bucket+"_"+lsn+"_r", "e")){
            throw new TTransportException("Logging rollback err:"+log.errmsg());
        }
        log.sync();   
    }
    
    public void delete(String bucket, String lsn){
        log.out(bucket+"_"+lsn);
    }
    
    public byte[] getLSNBuffer(String bucket, String lsn) {
        return log.get((bucket+"_"+lsn).getBytes());
    }
    
    public void setLSN(String bucket, String lsn){
        log.put("LSN_"+bucket, lsn);
    }
}