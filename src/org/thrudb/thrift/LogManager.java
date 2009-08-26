package org.thrudb.thrift;

import org.apache.thrift.transport.TTransportException;

public interface LogManager {

    void   setLSN(String bucket, String lsn);
    String getCurrentLSN(String bucket) throws TTransportException;
    String getNextLSN(String bucket) throws TTransportException;
    void log(String bucket, String lsn, byte[] buf) throws TTransportException;
    void commit(String bucket, String lsn)          throws TTransportException;
    void rollback(String bucket, String lsn)        throws TTransportException;
    void delete(String bucket, String lsn);
    byte[] getLSNBuffer(String bucket, String lsn);
}
