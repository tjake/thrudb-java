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
struct logEntry {
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
         * <pre>
         * Lists the names of the servers currently running
         * </pre>
        list<string>      getAvailibleServers();

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
        map<string,i64> getServiceStats(1:string server),

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
        list<logEntry>    getLogSince(1:string server, 2:string bucket, 3:string lsn, 4:i32 kbLimit);
}
