namespace cpp thrudb
namespace php Thrudb
namespace perl Thrudb
namespace java org.thrudb
namespace ruby Thrudb


/**
 * <pre>Represents a single log entry.
 *
 * This is captured off the wire so is the in the Thrift protocol used
 * during transfer
 *
 * @lsn     - the log sequence number of the message
 * @message - the message itself.</pre>
 */
struct LogEntry {
        1:string bucket,
        2:string lsn,
        3:binary message
}

/**
 * Base service calls all thrudb services must implement.
 *
 * Primarily for KPI and Replication purposes.
 *
 */
service Thrudb {

        /**
         *
         *Lists the names of the servers currently running
         */
        list<string>      getAvailibleServers();


		/**
		 *
		 *Changes the replication factor for a given bucket
		 *
		 */
		 boolean   setReplicationFactor(1:String bucket, 2:i32 factor);
		 
		 
		 /**
		  *
		  *Changes the partition factor for a given bucket
		  *
		  */
		 boolean   setPartitionFactor(1:String bucket, 2:i32 factor);       

        /**
         * <pre>
         * Retrieves a map of data about this service.
         *
         * There are many kinds of data:
         *
         * Service call counts -
         *                      All keys will start with "mc_", example: "mc_get"
         *                      All values will represent number of times invoked
         *
         * Service call message sizes -
         *                      All keys will start with "ms_"
         *                      All values will represent total bytes received
         *
         * Service memory/cpu usage, uptime and health -
         *                      key:"heap",  value:heapsize in kb
         *                      key:"cpu",   value:0-100 representing %cpu
         *                      key:"uptime",value:seconds since start
         *
         *
         * Note, this data is ephemeral so if the service is restarted the previous
         * stats are lost.</pre>
         */
        map<string,i64> getServiceStats(),

        /**
         * Acts as a noop, for debug and monitoring purposes.
         */
        void            ping(),

        /**
         * <pre>Will return a number of binary requests from the redo logs.
         *
         * @param lsn
         *            The log sequence number to start from (inclusive)
         * @param kbLimit
         *                        The max response size of the messages (not strict)</pre>
         */
        list<LogEntry>    getLogSince(1:string bucket, 2:string lsn, 3:i32 kbLimit);
}
