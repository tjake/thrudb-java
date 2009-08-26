package org.thrudb.thrudoc.zk;

import java.io.Serializable;

public class BucketInstance implements Serializable{
    private static final long serialVersionUID = 1L;
    public String  bucket;
    public String  ephemeralId;
    public String  instanceId;
	public String  shard;
	public String  lsn;
	public Boolean master = false;  
	
}
