package org.thrudb.thrudoc.zk;

import java.io.Serializable;

public class ServerInstance implements Serializable{
	public String instanceId;
	public String host;
	public int port;	
}
