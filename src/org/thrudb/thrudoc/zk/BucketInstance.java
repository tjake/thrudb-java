package org.thrudb.thrudoc.zk;

import java.io.Serializable;

public class BucketInstance implements Serializable{
	public String instanceId;
	public String shard;
	public Boolean master = false;
}
