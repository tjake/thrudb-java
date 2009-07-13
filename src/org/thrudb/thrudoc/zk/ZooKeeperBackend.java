package org.thrudb.thrudoc.zk;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.thrudb.LogEntry;
import org.thrudb.thrift.LogManager;
import org.thrudb.thrudoc.InvalidBucketException;
import org.thrudb.thrudoc.Thrudoc;
import org.thrudb.thrudoc.ThrudocBackend;
import org.thrudb.thrudoc.ThrudocException;
import org.thrudb.thrudoc.Thrudoc.Iface;
import org.thrudb.util.StaticHelpers;

/**
 * Zookeeper maintains the state of what instances are connected and what
 * resources they own. We are intentionally not going to shove a ton on
 * information into zookeeper because every write must propagate to all
 * watchers.
 * 
 * The information we store in zookeeper is: /serverList - the ephemeral
 * sequence of all connected instances
 * 
 * /bucketList/[bucket]/[instance] - the list of buckets and what instances
 * manage them
 * 
 * 
 * @author jake
 * 
 */
public final class ZooKeeperBackend implements Iface, Runnable, Watcher {

    private Logger logger = Logger.getLogger(getClass());
    private ZooKeeper zkServer;
    private String zkAddress;
    private int thrudocPort;

    private ThreadLocal<Map<ServerInstance, Thrudoc.Iface>> clientConnections = new ThreadLocal<Map<ServerInstance, Iface>>();

    private Thrudoc.Iface delegateHandler;

    public enum ZooState {
        STARTUP, SHUTDOWN, SERVER_TRANSITION, BUCKET_TRANSITION, ILLEGAL, UNKNOWN, CONNECTED, DISCONNECTED
    };

    private ZooState state = ZooState.DISCONNECTED;
    private CountDownLatch connectLatch = new CountDownLatch(1);

    /** node name for server list */
    private final String serverListNode = "/serverList";

    /** Keeps track of the instances connected */
    private List<String> serverList;
    private Map<String, ServerInstance> serverInfo;

    /** this instances unique id */
    private String instanceId;

    /** node name for space list */
    private final String bucketListNode = "/bucketList";
    private final String bucketMetaDataNode = "/bucketMetaDataNode";
    
    /** Keeps track of the spaces available on each instance */
    private List<String> bucketList;
    private Map<String, List<BucketInstance>> bucketInfo;
    
    
    /** Redo log **/
    LogManager logManager;

    public ZooKeeperBackend(String zkAddress, int port, Thrudoc.Iface delegateHandler, LogManager logManager) {
        this.zkAddress = zkAddress;
        this.delegateHandler = delegateHandler;
        this.thrudocPort = port;
        this.logManager = logManager;

        try {
            instanceId = new String(delegateHandler.get("thrudb", ThrudocBackend.SPECIAL_KEY));

            if (instanceId.equals("")) {
                throw new RuntimeException("Problem identifying this thrudb instance on zookeeper... bailing");
            }

        } catch (Exception e) {
            throw new RuntimeException("Problem identifying this thrudb instance on zookeeper... bailing");
        }

        this.serverList = new ArrayList<String>();
        this.serverInfo = new HashMap<String, ServerInstance>();
        this.bucketList = new ArrayList<String>();
        this.bucketInfo = new HashMap<String, List<BucketInstance>>();

        // start connector thread
        ExecutorService thread = Executors.newSingleThreadExecutor();
        thread.submit(this);
    }
    
    private void createServerList() throws KeeperException, InterruptedException {

        // create serverlistNode
        try {
            zkServer.create(serverListNode, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException e) {
            // this is fine.
        }

        InetSocketAddress address = new InetSocketAddress(thrudocPort);

        ServerInstance myInstance = new ServerInstance();

        myInstance.instanceId = instanceId;
        myInstance.host = address.getHostName();
        myInstance.port = thrudocPort;

        try {
            byte[] obj = StaticHelpers.toBytes(myInstance);

            // add myself to the instance list
            zkServer.create(serverListNode + "/id", obj, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // fetch the current list and start a monitor
        serverList = zkServer.getChildren(serverListNode, true);
        updateServerInfo();
    }

    public void updateServerInfo() {

        for (String server : serverList) {
            try {
                byte[] obj = zkServer.getData(serverListNode + "/" + server, false, null);

                ServerInstance info = (ServerInstance) StaticHelpers.fromBytes(obj);
                info.ephemeralId = server;
                
                serverInfo.put(info.instanceId, info);

            } catch (KeeperException e) {
                logger.error(e);
                throw new IllegalStateException(e);
            } catch (InterruptedException e) {
                logger.error(e);
                throw new IllegalStateException(e);
            } catch (ClassNotFoundException e) {
                logger.error(e);
                throw new IllegalStateException(e);
            } catch (IOException e) {
                logger.error(e);
                throw new IllegalStateException(e);
            }
        }

        state = ZooState.CONNECTED;
    }

    private void putBucketInZoo(String bucket) throws KeeperException, InterruptedException {
        // create the bucket node
        try {
            zkServer.create(bucketListNode + "/" + bucket, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException e) {
            // this is fine.
        }

        BucketInstance myBucket = new BucketInstance();
        myBucket.instanceId = instanceId;
        if(logManager != null){
            try{
                myBucket.lsn    = logManager.getCurrentLSN(bucket);
                logger.debug("Bucket LSN: "+myBucket.lsn);
            }catch(Exception e){
                throw new RuntimeException("Error reading log",e);
            }
        }

        try {
            byte[] obj = StaticHelpers.toBytes(myBucket);

            // add myself to the instance list
            zkServer.create(bucketListNode + "/" + bucket + "/id", obj, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void updateBucketList() throws InterruptedException, KeeperException {

        for (String bucket : bucketList) {
            List<String> bucketInstances = zkServer.getChildren(bucketListNode + "/" + bucket, true);

            List<BucketInstance> instanceList = new ArrayList<BucketInstance>();
            for (String instance : bucketInstances) {
                try {
                    byte[] obj = zkServer.getData(bucketListNode + "/" + bucket + "/" + instance, false, null);

                    BucketInstance info = (BucketInstance) StaticHelpers.fromBytes(obj);
                    info.ephemeralId = instance;
                    instanceList.add(info);

                } catch (KeeperException e) {
                    logger.error(e);
                    throw new IllegalStateException(e);
                } catch (InterruptedException e) {
                    logger.error(e);
                    throw new IllegalStateException(e);
                } catch (ClassNotFoundException e) {
                    logger.error(e);
                    throw new IllegalStateException(e);
                } catch (IOException e) {
                    logger.error(e);
                    throw new IllegalStateException(e);
                }
            }

            bucketInfo.put(bucket, instanceList);
        }

        state = ZooState.CONNECTED;
    }

    private void createBucketList() throws KeeperException, InterruptedException, TException, ThrudocException {

        // create the bucketlistNode
        try {
            zkServer.create(bucketListNode, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException e) {
            // this is fine.
        }

        Set<String> buckets = delegateHandler.get_bucket_list();

        for (String bucket : buckets) {
            putBucketInZoo(bucket);
        }

        // fetch the current list and start a monitor
        bucketList = zkServer.getChildren(bucketListNode, true);
        updateBucketList();
    }

    public void run() {
        while (true) {

            try {
                // first connect
                if (state == ZooState.DISCONNECTED) {
                    connectLatch = new CountDownLatch(1);
                    zkServer = new ZooKeeper(zkAddress, 3000, this);

                    connectLatch.await(); // connected

                    // setup monitors
                    this.createServerList();
                    this.createBucketList();
                }

                Thread.sleep(200);

            } catch (InterruptedException e) {
                logger.info("Interrupted");
                Thread.currentThread().interrupt();
                return;
            } catch (IOException e) {
                logger.error("IOException: " + e.getMessage());
            } catch (KeeperException e) {
                logger.error("KeeperException: " + e.getMessage());
            } catch (TException e) {
                logger.error("TException: " + e.getMessage());
            } catch (ThrudocException e) {
                logger.error("ThrudocException: " + e.getMessage());
            }
        }
    }

    public void process(WatchedEvent event) {

        String path = event.getPath();

        logger.debug(path);

        if (event.getType() == Event.EventType.None) {
            // the state of the connection has changed
            logger.debug("connection state change");

            switch (event.getState()) {

            case SyncConnected:
                state = ZooState.STARTUP;
                connectLatch.countDown();
                break;

            case Expired:
            case Disconnected:
                state = ZooState.DISCONNECTED;
                break;
            }
        } else {
            try {
                if (path != null && path.equals(serverListNode)) {
                    state = ZooState.SERVER_TRANSITION;
                    serverList = zkServer.getChildren(serverListNode, true);
                    this.updateServerInfo();
                }

                if (path != null && path.equals(bucketList)) {
                    state = ZooState.BUCKET_TRANSITION;
                    bucketList = zkServer.getChildren(bucketListNode, true);
                    updateBucketList();
                }
            } catch (Exception e) {
                logger.error(e.getMessage());
                throw new RuntimeException(e.getMessage());
            }
        }
    }

    /**
     * Find a bucket on a instance
     * 
     * @param bucket
     * @return
     */
    public ServerInstance getInstanceForBucket(String bucket, String key, boolean write) throws InvalidBucketException {

        if (state != ZooState.CONNECTED) {
            throw new IllegalStateException("ZooKeeper not ready");
        }

        List<BucketInstance> instances = bucketInfo.get(bucket);

        if (instances == null || instances.size() == 0) {
            throw new InvalidBucketException();
        }

        ServerInstance instance = serverInfo.get(instances.get(0).instanceId);

        return instance;
    }

    private Thrudoc.Iface getClient(ServerInstance instance) {

        Map<ServerInstance, Thrudoc.Iface> clientList = clientConnections.get();

        if (clientList == null) {
            clientList = new HashMap<ServerInstance, Thrudoc.Iface>();
        }

        Thrudoc.Iface client = clientList.get(instance);

        if (client == null) {

            if (instance.instanceId.equals(this.instanceId)) {
                client = delegateHandler;
            } else {
                TSocket socket = new TSocket(instance.host, instance.port);
                TTransport transport = new TFramedTransport(socket);
                TBinaryProtocol protocol = new TBinaryProtocol(transport);

                client = new Thrudoc.Client(protocol);

                try {
                    transport.open();
                } catch (TException e) {
                    throw new IllegalStateException("instance in zk but can't connect");
                }
            }

            clientList.put(instance, client);
        }

        return client;
    }

    public List<String> getAvailibleServers() throws TException {
        return serverList;
    }

    public List<LogEntry> getLogSince(String bucket, String lsn, int kbLimit) throws TException {
        // TODO Auto-generated method stub
        return null;
    }

    // should this return stats from all services?
    public Map<String, Long> getServiceStats() throws TException {
        return delegateHandler.getServiceStats();
    }

    public void ping() throws TException {

    }

    public String admin(String op, String data) throws ThrudocException, TException {
        return delegateHandler.admin(op, data);
    }

    public void create_bucket(String bucket) throws ThrudocException, TException {

        try {
            this.getInstanceForBucket(bucket, "", true);
            return; // awkward, but if no exception then bucket exists
        } catch (InvalidBucketException e) {
            // this is fine
        }

        // pick a random instance to start with
        int idx = Double.valueOf(Math.random()).intValue() * 10000 % serverInfo.values().size();
        ServerInstance instance = serverInfo.values().toArray(new ServerInstance[] {})[idx];

        getClient(instance).create_bucket(bucket);

        try {
            if (instance.instanceId.equals(instanceId)) {
                // publish myself to other instances
                logger.warn(instanceId + ":" + bucket);

                putBucketInZoo(bucket);
                bucketList = zkServer.getChildren(bucketListNode, true);
                updateBucketList();
            }
        } catch (Exception e) {
            throw new IllegalStateException("Problem updating bucketList", e);
        }

    }

    public int decr(String bucket, String key, int amount) throws ThrudocException, InvalidBucketException, TException {

        // first find the master
        ServerInstance instance = this.getInstanceForBucket(bucket, key, true);

        return getClient(instance).decr(bucket, key, amount);
    }

    public void delete_bucket(String bucket) throws ThrudocException, TException {

        try {
            ServerInstance instance = this.getInstanceForBucket(bucket, "", true);

            if (instance == null && state == ZooState.CONNECTED)
                return;

            getClient(instance).delete_bucket(bucket);

            // FIXME: this should update zk bucketList
            BucketInstance bucketInstance = null;
            for (BucketInstance tmpBucket : bucketInfo.get(bucket)) {
                if (tmpBucket.instanceId.equals(instanceId)) {
                    bucketInstance = tmpBucket;
                    break;
                }
            }

            try {
                if (bucketInstance != null) {
                    zkServer.delete(bucketListNode + "/" + bucket + "/" + bucketInstance.ephemeralId, -1);
                    bucketList = zkServer.getChildren(bucketListNode, true);
                    updateBucketList();
                }
            } catch (Exception e) {
                throw new IllegalStateException("Problem deleting ephemeral node", e);
            }

        } catch (InvalidBucketException e) {
            // this is fine
        }
    }

    public Set<String> get_bucket_list() throws ThrudocException, TException {
        Set<String> bucketSet = new HashSet<String>();

        for (String bucket : bucketList) {
            bucketSet.add(bucket);
        }

        return bucketSet;
    }

    public byte[] get(String bucket, String key) throws ThrudocException, InvalidBucketException, TException {

        return getClient(getInstanceForBucket(bucket, key, false)).get(bucket, key);

    }

    public int incr(String bucket, String key, int amount) throws ThrudocException, InvalidBucketException, TException {

        return getClient(getInstanceForBucket(bucket, key, true)).decr(bucket, key, amount);
    }

    public void insert_at(String bucket, String key, byte[] value, int pos) throws ThrudocException, InvalidBucketException, TException {

        getClient(getInstanceForBucket(bucket, key, true)).insert_at(bucket, key, value, pos);
    }

    public int length(String bucket, String key) throws ThrudocException, InvalidBucketException, TException {

        return getClient(getInstanceForBucket(bucket, key, false)).length(bucket, key);
    }

    public byte[] pop_back(String bucket, String key) throws ThrudocException, InvalidBucketException, TException {

        return getClient(getInstanceForBucket(bucket, key, true)).pop_back(bucket, key);
    }

    public byte[] pop_front(String bucket, String key) throws ThrudocException, InvalidBucketException, TException {

        return getClient(getInstanceForBucket(bucket, key, true)).pop_front(bucket, key);
    }

    public void push_back(String bucket, String key, byte[] value) throws ThrudocException, InvalidBucketException, TException {

        getClient(getInstanceForBucket(bucket, key, true)).push_back(bucket, key, value);
    }

    public void push_front(String bucket, String key, byte[] value) throws ThrudocException, InvalidBucketException, TException {

        getClient(getInstanceForBucket(bucket, key, true)).push_front(bucket, key, value);
    }

    public void put(String bucket, String key, byte[] value) throws ThrudocException, InvalidBucketException, TException {

        getClient(getInstanceForBucket(bucket, key, true)).put(bucket, key, value);
    }

    public List<byte[]> range(String bucket, String key, int start, int end) throws ThrudocException, InvalidBucketException, TException {

        return getClient(getInstanceForBucket(bucket, key, false)).range(bucket, key, start, end);
    }

    public byte[] remove_at(String bucket, String key, int pos) throws ThrudocException, InvalidBucketException, TException {

        return getClient(getInstanceForBucket(bucket, key, true)).remove_at(bucket, key, pos);
    }

    public void remove(String bucket, String key) throws ThrudocException, InvalidBucketException, TException {

        getClient(getInstanceForBucket(bucket, key, true)).remove(bucket, key);
    }

    public void replace_at(String bucket, String key, byte[] value, int pos) throws ThrudocException, InvalidBucketException, TException {

        getClient(getInstanceForBucket(bucket, key, true)).replace_at(bucket, key, value, pos);
    }

    public byte[] retrieve_at(String bucket, String key, int pos) throws ThrudocException, InvalidBucketException, TException {
        return getClient(getInstanceForBucket(bucket, key, false)).retrieve_at(bucket, key, pos);
    }

    // FIXME: this should scan all shards
    public List<String> scan(String bucket, String seed, int limit) throws ThrudocException, InvalidBucketException, TException {
        return getClient(getInstanceForBucket(bucket, seed, false)).scan(bucket, seed, limit);
    }

    public void setPartitionFactor(String bucket, int factor) throws TException {
        // TODO Auto-generated method stub
        
    }

    public void setReplicationFactor(String bucket, int factor) throws TException {
        try {
            String node = bucketMetaDataNode+"/"+bucket;
            Stat exists = zkServer.exists(node, false);
            
            BucketMetaData bucketMetaData = null;
            
            if(exists != null){
                byte[] obj = zkServer.getData(node, false, null);
                bucketMetaData = (BucketMetaData) StaticHelpers.fromBytes(obj);
            }
            
            if(bucketMetaData == null){
                bucketMetaData = new BucketMetaData();
            }
            
            bucketMetaData.replicas = factor;
            
            if(exists == null){
                zkServer.create(node, StaticHelpers.toBytes(bucketMetaData), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }else{
                zkServer.setData(node, StaticHelpers.toBytes(bucketMetaData), -1);
            }
            
        } catch (Exception e){
            throw new TException(e);
        }   
    }

}
