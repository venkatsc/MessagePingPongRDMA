package com.message;

import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.RdmaEndpointFactory;
import com.ibm.disni.RdmaServerEndpoint;
import com.ibm.disni.verbs.IbvMr;
import com.ibm.disni.verbs.IbvWC;
import com.ibm.disni.verbs.RdmaCmId;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

public class RdmaServer implements RdmaEndpointFactory<RdmaShuffleServerEndpoint>, Runnable {
    //private static final Logger LOG = LoggerFactory.getLogger(RdmaServer.class);
    private RdmaActiveEndpointGroup<RdmaShuffleServerEndpoint> endpointGroup;
    private final RdmaConfig rdmaConfig;
    private int workRequestId = 1;
    private RdmaServerEndpoint<RdmaShuffleServerEndpoint> serverEndpoint;
    private InetSocketAddress address;
    private int bufferSize = 100; // Todo: set default buffer size
    private ByteBuffer sendBuffer; // Todo: add buffer manager with multiple buffers
    private IbvMr registeredSendMemory; // Registered memory for the above buffer
    private ByteBuffer receiveBuffer; // Todo: add buffer manager with multiple buffers
    private IbvMr registeredReceiveMemory;
    private BufferProvider bufferProvider;
    private CountDownLatch latch;

    public RdmaShuffleServerEndpoint getClientEndpoint() {
        return clientEndpoint;
    }

    private RdmaShuffleServerEndpoint clientEndpoint;


    private void registerMemoryRegions(RdmaServerEndpoint<RdmaShuffleServerEndpoint> endpoint) throws IOException {
        long start = System.nanoTime();
        this.sendBuffer = bufferProvider.getServerSendBuffer(); // allocate buffer
        for (int i = 0; i < rdmaConfig.getThrowAwayBufferCount(); i++) {
            ByteBuffer throwAway = ByteBuffer.allocateDirect(1 * 1024 * 1024);
            endpoint.registerMemory(throwAway).execute().getMr();
        }
        this.receiveBuffer = bufferProvider.getServerReceiveBuffer();
        this.registeredReceiveMemory = endpoint.registerMemory(receiveBuffer).execute().getMr();
        this.registeredSendMemory = endpoint.registerMemory(sendBuffer).execute().getMr();
        bufferProvider.setRegisteredClientReceiveMemory(endpoint.registerMemory(bufferProvider.getClientReceiveBuffer()).execute().getMr());
        bufferProvider.setRegisteredClientSendMemory(endpoint.registerMemory(bufferProvider.getClientSendBuffer()).execute().getMr());
        long end = System.nanoTime();
        System.out.println("Server: Memory resgistration time for " + rdmaConfig.getThrowAwayBufferCount() + "MB (in seconds): " + ((end
                - start) / (1000.0*1000*1000)));;
    }
//private PartitionRequestServerHandler serverHandler;
//private NettyBufferPool bufferPool;

    /**
     * Creates the Queue pair endpoint and waits for the incoming connections
     *
     * @param idPriv
     * @param serverSide
     * @return
     * @throws java.io.IOException
     */
    public RdmaShuffleServerEndpoint createEndpoint(RdmaCmId idPriv, boolean serverSide) throws IOException {
        return new RdmaShuffleServerEndpoint(endpointGroup, idPriv, serverSide, 100);
    }

    public RdmaServer(RdmaConfig rdmaConfig,BufferProvider bufferProvider, CountDownLatch latch) {
        this.rdmaConfig = rdmaConfig;
        this.bufferProvider=bufferProvider;
        this.latch = latch;
    }

    public void run() {
        //create a EndpointGroup. The RdmaActiveEndpointGroup contains CQ processing and delivers CQ event to the
        // endpoint.dispatchCqEvent() method.

        try {
            endpointGroup = new RdmaActiveEndpointGroup<RdmaShuffleServerEndpoint>(1000, true, 128, 4, 128);
            endpointGroup.init(this);
            //create a server endpoint
            serverEndpoint = endpointGroup.createServerEndpoint();


            //we can call bind on a server endpoint, just like we do with sockets
//		InetAddress ipAddress = InetAddress.getByName(host);
            address = new InetSocketAddress(rdmaConfig.getServerAddress(), rdmaConfig.getServerPort());
            try {
                serverEndpoint.bind(address, 10);
                this.registerMemoryRegions(serverEndpoint);
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (latch != null) {
                latch.countDown();
            }
            System.out.println("SimpleServer::servers bound to address " + address.toString());
            int conns = 0;
            while (conns < 2) {
                long start = System.currentTimeMillis();
                //we can accept new connections
                clientEndpoint = serverEndpoint.accept();
                clientEndpoint.setReceiveBuffer(receiveBuffer);
                clientEndpoint.setSendBuffer(sendBuffer);
                clientEndpoint.setRegisteredReceiveMemory(registeredReceiveMemory);
                clientEndpoint.setRegisteredSendMemory(registeredSendMemory);
                conns++;
                RdmaSendReceiveUtil.postReceiveReq(clientEndpoint, ++workRequestId);
                System.out.println("\n\n\nAccepted connection " + conns + "\n\n\n");

                //we have previously passed our own endpoint factory to the group, therefore new endpoints will be of type

                // CustomServerEndpoint
                int i = 0;
                while (i < 50) {
//                long start = System.nanoTime();
                    IbvWC wc = clientEndpoint.getWcEvents().take();
//                long end = System.nanoTime();
//                System.out.println("Server Latency to pop-element out of queue "+ (end-start));
                    if (IbvWC.IbvWcOpcode.valueOf(wc.getOpcode()) == IbvWC.IbvWcOpcode.IBV_WC_RECV) {
                        i++;
                        if (wc.getStatus() != IbvWC.IbvWcStatus.IBV_WC_SUCCESS.ordinal()) {
                            System.out.println("Receive posting failed. reposting new receive request");
//                        RdmaSendReceiveUtil.postReceiveReq(clientEndpoint, ++workRequestId);
                        } else { // first receive succeeded. Read the data and repost the next message
                            RdmaMessage.PartitionRequest clientRequest = (RdmaMessage.PartitionRequest) RdmaMessage
                                    .PartitionRequest.readFrom(clientEndpoint.getReceiveBuffer());

                            System.out.println("client requested partition id: " + clientRequest.getPartitionId());
                            clientEndpoint.getReceiveBuffer().clear();
                            RdmaMessage.PartitionRequest request = new RdmaMessage.PartitionRequest(clientRequest
                                    .getPartitionId());
                            clientEndpoint.getSendBuffer().clear();
                            request.writeTo(clientEndpoint.getSendBuffer());
                            RdmaSendReceiveUtil.postReceiveReq(clientEndpoint, ++workRequestId); // post next receive
                            RdmaSendReceiveUtil.postSendReq(clientEndpoint, ++workRequestId);
                        }
                    } else if (IbvWC.IbvWcOpcode.valueOf(wc.getOpcode()) == IbvWC.IbvWcOpcode.IBV_WC_SEND) {
                        if (wc.getStatus() != IbvWC.IbvWcStatus.IBV_WC_SUCCESS.ordinal()) {
                            System.out.println("Send failed. reposting new send request request");
//                        RdmaSendReceiveUtil.postSendReq(clientEndpoint, ++workRequestId);
                        }
                        clientEndpoint.getSendBuffer().clear();
                        // Send succeed does not require any action
                    } else {
                        System.out.println("failed to match any condition " + wc.getOpcode());
                    }
                }
                long end = System.currentTimeMillis();
                System.out.println("total time: "+ (end-start));
                clientEndpoint.close();
            }
            //close everything
//        System.out.println("group closed");
            this.shutdown();
//		System.exit(0);
        }catch(Exception e){
            e.printStackTrace();
        }

    }

    public static void main(String[] args) throws Exception {
        CmdLineCommon cmdLine = new CmdLineCommon("RdmaServer");
        try {
            BasicConfigurator.configure();
            org.apache.log4j.Logger.getRootLogger().setLevel(Level.INFO);
            cmdLine.parse(args);
            cmdLine.parse(args);
        } catch (ParseException e) {
            cmdLine.printHelp();
            System.exit(-1);
        }
        BufferProvider bufferProvider= new BufferProvider();
        RdmaConfig rdmaConfig = new RdmaConfig(InetAddress.getByName(cmdLine.getIp()), cmdLine.getPort(), cmdLine
                .getThrowAwayBufferCount());
        RdmaServer server = new RdmaServer(rdmaConfig,bufferProvider,null); //TODO (venkat): it should not
        // be null
        Thread t = new Thread(server);
        t.start();
    }

    public void shutdown() {
        try {
            clientEndpoint.close();
//			System.out.println("client endpoint closed");
            serverEndpoint.close();
            System.out.println("server endpoint closed");
            endpointGroup.close();
        } catch (Exception e) {
//            LOG.error(e.getMessage());
            e.printStackTrace();
        }
    }
}
