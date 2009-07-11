package org.thrudb.thrudoc.zk;

import java.io.Serializable;

public class BucketInstance implements Serializable{
	public String  ephemeralId;
    public String  instanceId;
	public String  shard;
	public Boolean master = false;
}
