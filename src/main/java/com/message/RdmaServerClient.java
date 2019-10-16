package com.message;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;

import java.net.InetAddress;
import java.util.concurrent.CountDownLatch;

public class RdmaServerClient {

    public static void main(String[] args) throws Exception {
        CmdLineCommon cmdLine = new CmdLineCommon("RdmaClient");
        try {
            BasicConfigurator.configure();
            org.apache.log4j.Logger.getRootLogger().setLevel(Level.INFO);
            cmdLine.parse(args);
        } catch (org.apache.commons.cli.ParseException e) {
            cmdLine.printHelp();
            System.exit(-1);
        }
        BufferProvider bufferProvider= new BufferProvider();
        RdmaConfig rdmaConfig = new RdmaConfig(InetAddress.getByName(cmdLine.getIp()), cmdLine.getPort(),cmdLine.getThrowAwayBufferCount());
        CountDownLatch latch=new CountDownLatch(1);
        RdmaServer server = new RdmaServer(rdmaConfig,bufferProvider,latch); // TODO: need to pass client partition handler
        Thread s = new Thread(server);
        s.start();
        latch.await();
        RdmaClient client = new RdmaClient(rdmaConfig,bufferProvider); // TODO: need to pass client partition handler
        Thread c = new Thread(client);
        c.start();
    }
}
