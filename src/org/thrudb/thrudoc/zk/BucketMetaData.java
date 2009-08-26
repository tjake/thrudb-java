package org.thrudb.thrudoc.zk;

import java.io.Serializable;

public class BucketMetaData implements Serializable {
    private static final long serialVersionUID = 1L;
    String bucket;
    int    replicas = 1;
    int    shards   = 1;
}
