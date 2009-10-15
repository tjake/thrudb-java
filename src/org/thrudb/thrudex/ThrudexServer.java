package org.thrudb.thrudex;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.THsHaServer.Options;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportFactory;
import org.thrudb.thrudex.lucene.ThrudexLuceneHandler;

public class ThrudexServer {

    private String indexRoot;
    private int port;
    private int threadCount;
    private boolean framedTransport;
    private ThrudexBackendType backendType;
    private static final Logger logger = Logger.getLogger(ThrudexServer.class);

    public String getIndexRoot() {
        return indexRoot;
    }

    public void setIndexRoot(String indexRoot) {
        this.indexRoot = indexRoot;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getThreadCount() {
        return threadCount;
    }

    public void setThreadCount(int threadCount) {
        this.threadCount = threadCount;
    }

    public boolean isFramedTransport() {
        return framedTransport;
    }

    public void setFramedTransport(boolean framedTransport) {
        this.framedTransport = framedTransport;
    }

    public ThrudexBackendType getBackendType() {
        return backendType;
    }

    public void setBackendType(ThrudexBackendType backendType) {
        this.backendType = backendType;
    }
    
    public void start() {
        // Start the server
        try {
            logger.info("Starting thrudex service");
            
            
            logger.info("index root: " + this.getIndexRoot());
            logger.info("backend type: " + this.getBackendType());
            logger.info("service port: " + this.getPort());
            logger.info("thread count: " + this.getThreadCount());
            logger.info("framed transport: " + this.isFramedTransport());
            

            // Processor
            TProcessor processor = new Thrudex.Processor(new ThrudexLuceneHandler(indexRoot,backendType));

            
            // Protocol factory
            TProtocolFactory tProtocolFactory = new TBinaryProtocol.Factory();
            
            // Transport factory
            TTransportFactory inTransportFactory, outTransportFactory;
            
            TServer server;
            if (framedTransport) {
                // Transport
                TNonblockingServerTransport serverSocket = new TNonblockingServerSocket(port);
                Options opt = new Options();
                opt.maxWorkerThreads = threadCount;

                // Server
                server = new THsHaServer(processor, serverSocket, opt);
            }else{
                
                TServerSocket serverSocket = new TServerSocket(port);
                    
                
                inTransportFactory = new TTransportFactory();
                outTransportFactory = new TTransportFactory();
                
                // ThreadPool Server
                TThreadPoolServer.Options options = new TThreadPoolServer.Options();
                options.maxWorkerThreads = threadCount;
                server = new TThreadPoolServer(new TProcessorFactory(processor),
                                                     serverSocket,
                                                     inTransportFactory,
                                                     outTransportFactory,
                                                     tProtocolFactory,
                                                     tProtocolFactory,
                                                     options);
            }
                
            
            // Serve
            server.serve();

        } catch (Exception e) {
            logger.error("Exception caught:", e);
        }
    }

    /**
     * @param args
     */
    public static void main(String[] args) {

        File propFile = null;
        Properties properties = new Properties();

        // check args
        for (int i = 0; i < args.length; i++) {

            if (args[i].equalsIgnoreCase("-f")) {
                if (i + 1 >= args.length) {
                    printUsageAndExit();
                } else {
                    propFile = new File(args[i + 1]);
                    if (propFile.exists() && propFile.canRead() && propFile.isFile()) {
                        try {
                            properties.load(new FileInputStream(propFile));
                        } catch (Exception e) {
                            System.err.println("Error while loading properties file: " + args[i + 1] + " (" + e.getMessage() + ")");
                            System.exit(1);
                        }
                    } else {
                        System.err.println("Invalid properties file: " + args[i + 1]);
                        System.exit(1);
                    }
                }
            }
        }

        if (properties.isEmpty()) {
            printUsageAndExit();
        }

        PropertyConfigurator.configure(propFile.getAbsolutePath());

        ThrudexServer thrudexServer = new ThrudexServer();

        if (!properties.containsKey("INDEX_ROOT")) {
            System.err.println("INDEX_ROOT Property Required");
            System.exit(1);
        }

        String indexRoot = properties.getProperty("INDEX_ROOT");
        thrudexServer.setIndexRoot(indexRoot);

        int port = Integer.valueOf(properties.getProperty("SERVER_PORT", "9090"));
        thrudexServer.setPort(port);

        int threadCount = Integer.valueOf(properties.getProperty("THREAD_COUNT", "5"));
        thrudexServer.setThreadCount(threadCount);

        boolean framedTransport = Boolean.valueOf(properties.getProperty("FRAMED_TRANSPORT", "true"));
        thrudexServer.setFramedTransport(framedTransport);

        ThrudexBackendType backendType = ThrudexBackendType.valueOf(properties.getProperty("BACKEND_TYPE","REALTIME"));
        thrudexServer.setBackendType(backendType);
        
        thrudexServer.start();
    }

    private static void printUsageAndExit() {
        System.err.println("Usage : thrudex -f thrudex.properties");
        System.exit(1);
    }

   
}
