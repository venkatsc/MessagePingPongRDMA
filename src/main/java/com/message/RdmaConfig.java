package com.message;

import java.net.InetAddress;


public class RdmaConfig {
    private final InetAddress serverAddress;
    private int serverPort;

    private int memorySegmentSize;
    private int cqSize;

//    public RdmaConfig(
//            InetAddress serverAddress,
//            int serverPort,
//            int memorySegmentSize,
//            Configuration config, int cqSize) {
//
//        this.serverAddress = checkNotNull(serverAddress);
//
//        checkArgument(serverPort >= 0 && serverPort <= 65536, "Invalid port number.");
//        this.serverPort = serverPort;
//
//        checkArgument(memorySegmentSize > 0, "Invalid memory segment size.");
//        this.memorySegmentSize = memorySegmentSize;
//        checkArgument(cqSize > 0, "Invalid completion queue size.");
//        this.cqSize = cqSize;
//        this.config = checkNotNull(config);
//
//        LOG.info(this.toString());
//    }

    // TODO: remove (only for stand-alone running of server
    public RdmaConfig(
            InetAddress serverAddress,
            int serverPort) {
        this.serverAddress = serverAddress;
        this.serverPort = serverPort;
    }

    public InetAddress getServerAddress() {
        return serverAddress;
    }

    public int getServerPort() {
        return serverPort;
    }

    public int getMemorySegmentSize() {
        return memorySegmentSize;
    }

    public int getCqSize() {
        return cqSize;
    }

//    public Configuration getConfig() {
//        return config;
//    }

}
