package org.thrudb.thrudoc.zk;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.transport.TMemoryBuffer;
import org.thrudb.LogEntry;
import org.thrudb.thrift.LogManager;
import org.thrudb.thrudoc.Thrudoc;

public class ReplicationManager implements Runnable {
    private static Logger logger = Logger.getLogger(ReplicationManager.class);
    
    //Slave, Master
    private Map<BucketInstance, BucketInstance> replicaMap = new ConcurrentHashMap<BucketInstance, BucketInstance>();
    private ZooKeeperBackend  zkBackend;
    private LogManager        logManager;
    private Thrudoc.Processor processor;
    private TMemoryBuffer     buffer;
    private TBinaryProtocol   protocol;
    
    public ReplicationManager(ZooKeeperBackend zkBackend) {
        this.zkBackend  = zkBackend;
        this.logManager = zkBackend.getLogManager();
        
        processor = new Thrudoc.Processor(zkBackend.getDelegateHandler());
        buffer    = new TMemoryBuffer(100);
        protocol  = new TBinaryProtocol(buffer);
    }
    
    public void startReplication(BucketInstance master, BucketInstance slave) {
        if(slave.instanceId.equals(zkBackend.getInstanceId())){
            throw new IllegalStateException("slave not owned by this isntance");
        }
        
        replicaMap.put(slave, master);
    }

    public void stopReplication(BucketInstance slave) {
        replicaMap.remove(slave);
    }

    public void run() {
        while (true) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {

            }
            
            for(Map.Entry<BucketInstance, BucketInstance> entry : replicaMap.entrySet()){
              
                try{
                    BucketInstance master = entry.getKey();
                    BucketInstance slave  = entry.getValue();
     
                    Thrudoc.Iface masterConn = zkBackend.getClient(zkBackend.getServerForBucketInstance(master));

                    List<LogEntry> logEntries = masterConn.getLogSince(slave.bucket, slave.lsn, 1000);
                    
                    for(LogEntry logEntry : logEntries) {
                       
                        buffer.flush();
                        buffer.write(logEntry.getMessage());
                        
                        logManager.log(logEntry.getBucket(), logEntry.getLsn(), logEntry.getMessage());
                        logManager.setLSN(logEntry.getBucket(), logEntry.getLsn());
                        
                        processor.process(protocol, protocol);
                        
                        
                        TMessage msg = protocol.readMessageBegin();
                        if(msg.type == TMessageType.EXCEPTION){
                            logger.error("Exception encountered performing replication action");
                            break;
                        }
                        
                        
                    }
                    
                }catch(Throwable t){
                    logger.warn(t);
                }
                    
            }
            
        }
    }

}
