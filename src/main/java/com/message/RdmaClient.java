package com.message;

import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.RdmaEndpointFactory;
import com.ibm.disni.verbs.IbvMr;
import com.ibm.disni.verbs.IbvWC;
import com.ibm.disni.verbs.RdmaCmId;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

public class RdmaClient implements RdmaEndpointFactory<RdmaShuffleClientEndpoint>, Runnable {
    //    private static final Logger LOG = LoggerFactory.getLogger(RdmaClient.class);
    private int bufferSize = 100;
    RdmaActiveEndpointGroup<RdmaShuffleClientEndpoint> endpointGroup;
    private final RdmaConfig rdmaConfig;
    private int workRequestId = 1;
    //    private NettyBufferPool bufferPool;
    private ByteBuffer receiveBuffer; // Todo: add buffer manager with multiple buffers
    private IbvMr registeredReceiveMemory; // Registered memory for the above buffer

    private ByteBuffer sendBuffer;
    private IbvMr registeredSendMemory;

    private BufferProvider bufferProvider;

    public RdmaShuffleClientEndpoint getEndpoint() {
        return endpoint;
    }

    private RdmaShuffleClientEndpoint endpoint;
//    private PartitionRequestClientHandler clientHandler;

    public RdmaClient(RdmaConfig rdmaConfig, BufferProvider bufferProvider) {
        this.rdmaConfig = rdmaConfig;
        this.bufferProvider = bufferProvider;
//        this.clientHandler = clientHandler;
//        this.bufferPool=bufferPool;
    }

    private void initializeBuffers() {
        this.sendBuffer = bufferProvider.getClientSendBuffer();
        this.receiveBuffer = bufferProvider.getClientReceiveBuffer();
        this.registeredSendMemory = bufferProvider.getRegisteredClientSendMemory();
        this.registeredReceiveMemory = bufferProvider.getRegisteredClientReceiveMemory();
    }

    private void registerMemoryRegions() throws IOException {
        long start = System.nanoTime();
        this.receiveBuffer = bufferProvider.getClientReceiveBuffer(); // allocate buffer
        this.registeredReceiveMemory = endpoint.registerMemory(receiveBuffer).execute().getMr(); // register the send
        // buffer
        for (int i = 0; i < rdmaConfig.getThrowAwayBufferCount(); i++) {
            ByteBuffer throwAway = ByteBuffer.allocateDirect(1 * 1024 * 1024);
            endpoint.registerMemory(throwAway).execute().getMr();
        }
        this.sendBuffer = bufferProvider.getClientSendBuffer(); // allocate buffer
        this.registeredSendMemory = endpoint.registerMemory(sendBuffer).execute().getMr();
        // register server memory on the client protection domain (PD)
        endpoint.registerMemory(bufferProvider.getServerReceiveBuffer()).execute().getMr();
        endpoint.registerMemory(bufferProvider.getServerSendBuffer()).execute().getMr();
        long end = System.nanoTime();
        System.out.println("Client: Memory resgistration time for " + rdmaConfig.getThrowAwayBufferCount() + "MB (in " +
                "seconds): " + (end
                - start) / (1000.0 * 1000 * 1000));
    }

    public RdmaShuffleClientEndpoint createEndpoint(RdmaCmId idPriv, boolean serverSide) throws IOException {
        return new RdmaShuffleClientEndpoint(endpointGroup, idPriv, serverSide, 100);
    }

    public void run() {
        //create a EndpointGroup. The RdmaActiveEndpointGroup contains CQ processing and delivers CQ event to the
        // endpoint.dispatchCqEvent() method.
        try {
            System.out.println("Starting client");
            //connect to the server
//		InetAddress ipAddress = InetAddress.getByName(host);
            endpointGroup = new RdmaActiveEndpointGroup<RdmaShuffleClientEndpoint>(1000, true, 128, 4, 128);
            endpointGroup.init(this);
            //we have passed our own endpoint factory to the group, therefore new endpoints will be of type
            // CustomClientEndpoint
            //let's create a new client endpoint
            for (int con = 0; con < 2; con++) {
                endpoint = endpointGroup.createEndpoint();
                InetSocketAddress address = new InetSocketAddress(rdmaConfig.getServerAddress(), rdmaConfig
                        .getServerPort());
                endpoint.connect(address, 1000);
                Thread.sleep(1000);
                System.out.println("\n\n\nClient connection " + con);
                // No need of registration on client connection in same JVM, it seems the PD is same for both server
                // and client inside same JVM
                // Disable it during next iteration of the code
//                if (con == 0) {
//                    System.out.println("Registering memory");
//                    long startTime = System.nanoTime();
//                    registerMemoryRegions();
//                    long endTime = System.nanoTime();
//                    System.out.println("Latency for memory registration " + (endTime - startTime));
//                }
                initializeBuffers();
                endpoint.setReceiveBuffer(receiveBuffer);
                endpoint.setSendBuffer(sendBuffer);
                endpoint.setRegisteredReceiveMemory(registeredReceiveMemory);
                endpoint.setRegisteredSendMemory(registeredSendMemory);

                // Post receive request
//		RdmaSendReceiveUtil.postReceiveReq(endpoint,++workRequestId);
                // TODO: give time for server to post Receive Work request RWR
                System.out.println("SimpleClient::client channel set up ");
                int i = 0;
                RdmaSendReceiveUtil.postReceiveReq(endpoint, ++workRequestId);
                RdmaMessage.PartitionRequest request = new RdmaMessage.PartitionRequest(i);
                endpoint.getSendBuffer().clear();
                request.writeTo(endpoint.getSendBuffer());
                RdmaSendReceiveUtil.postSendReq(endpoint, ++workRequestId);
                while (i < 50) {
//                long start = System.nanoTime();
                    IbvWC wc = endpoint.getWcEvents().take();
//                long end = System.nanoTime();
//                System.out.println("Client Latency to pop-element out of queue " + (end - start));
                    if (IbvWC.IbvWcOpcode.valueOf(wc.getOpcode()) == IbvWC.IbvWcOpcode.IBV_WC_RECV) {
                        i++;
                        if (wc.getStatus() != IbvWC.IbvWcStatus.IBV_WC_SUCCESS.ordinal()) {
                            System.out.println("Receive posting failed. reposting new receive request");
                            RdmaSendReceiveUtil.postReceiveReq(endpoint, ++workRequestId);
                        } else { // first receive succeeded. Read the data and repost the next message
                            RdmaMessage.PartitionResponse response = (RdmaMessage.PartitionResponse) RdmaMessage
                                    .PartitionResponse.readFrom(endpoint.getReceiveBuffer());
                            System.out.println("Response partition id: " + response.getPartitionId());
                            endpoint.getReceiveBuffer().clear();
                            if (i == 50) {
                                continue; // we just want to post requests until 49
                            }
                            RdmaSendReceiveUtil.postReceiveReq(endpoint, ++workRequestId);

                            RdmaMessage.PartitionRequest request1 = new RdmaMessage.PartitionRequest(i);
                            endpoint.getSendBuffer().clear();
                            request1.writeTo(endpoint.getSendBuffer());
                            RdmaSendReceiveUtil.postSendReq(endpoint, ++workRequestId);
                        }
                    } else if (IbvWC.IbvWcOpcode.valueOf(wc.getOpcode()) == IbvWC.IbvWcOpcode.IBV_WC_SEND) {
                        if (wc.getStatus() != IbvWC.IbvWcStatus.IBV_WC_SUCCESS.ordinal()) {
                            System.out.println("Send failed. reposting new send request request");
                            RdmaSendReceiveUtil.postSendReq(endpoint, ++workRequestId);
                        }
                        endpoint.getSendBuffer().clear();
                        // Send succeed does not require any action
                    } else {
                        System.out.println("failed to match any condition " + wc.getOpcode());
                    }
                }
                endpoint.close();
            }
            this.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
//		System.exit(0);
    }

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
        BufferProvider bufferProvider = new BufferProvider();
        RdmaConfig rdmaConfig = new RdmaConfig(InetAddress.getByName(cmdLine.getIp()), cmdLine.getPort(), cmdLine
                .getThrowAwayBufferCount());
        RdmaClient client = new RdmaClient(rdmaConfig, bufferProvider); // TODO: need to pass client partition handler
        Thread t = new Thread(client);
        t.start();
    }

    public void shutdown() {
        try {
            endpoint.close();
            System.out.println("client endpoint closed");
            System.out.println("server endpoint closed");
            endpointGroup.close();
        } catch (Exception e) {
//            LOG.error(e.getMessage());
            e.printStackTrace();
        }
    }
}
