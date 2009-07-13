package org.thrudb.thrudoc.zk;

import java.io.Serializable;

public class ServerInstance implements Serializable{
    private static final long serialVersionUID = 1L;
    public String ephemeralId;
	public String instanceId;
	public String host;
	public int port;	
}
