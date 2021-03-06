package com.message;

import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.RdmaEndpointFactory;
import com.ibm.disni.verbs.IbvWC;
import com.ibm.disni.verbs.RdmaCmId;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

public class RdmaClient implements RdmaEndpointFactory<RdmaShuffleEndpoint> {
    //    private static final Logger LOG = LoggerFactory.getLogger(RdmaClient.class);
    RdmaActiveEndpointGroup<RdmaShuffleEndpoint> endpointGroup;
    private final RdmaConfig rdmaConfig;
    private int workRequestId = 1;
//    private NettyBufferPool bufferPool;

    public RdmaShuffleEndpoint getEndpoint() {
        return endpoint;
    }

    private RdmaShuffleEndpoint endpoint;
//    private PartitionRequestClientHandler clientHandler;

    public RdmaClient(RdmaConfig rdmaConfig) {
        this.rdmaConfig = rdmaConfig;
//        this.clientHandler = clientHandler;
//        this.bufferPool=bufferPool;
    }

    public RdmaShuffleEndpoint createEndpoint(RdmaCmId idPriv, boolean serverSide) throws IOException {
        return new RdmaShuffleEndpoint(endpointGroup, idPriv, serverSide, 4096);
    }

    public void run() throws Exception {
        //create a EndpointGroup. The RdmaActiveEndpointGroup contains CQ processing and delivers CQ event to the
        // endpoint.dispatchCqEvent() method.
        System.out.println("Starting client");
        //connect to the server
//		InetAddress ipAddress = InetAddress.getByName(host);
        endpointGroup = new RdmaActiveEndpointGroup<RdmaShuffleEndpoint>(1000, true, 128, 4, 128,true);
        endpointGroup.init(this);
        //we have passed our own endpoint factory to the group, therefore new endpoints will be of type
        // CustomClientEndpoint
        //let's create a new client endpoint
        endpointGroup.getConnParam().setRnr_retry_count((byte)7);
//        endpoint = endpointGroup.createEndpoint();
        for (int c=0;c<2;c++) {
            System.out.println("\n\n\n\nclient iteration "+c +"\n\n");
            InetSocketAddress address = new InetSocketAddress(rdmaConfig.getServerAddress(), rdmaConfig.getServerPort());
            endpoint = endpointGroup.createEndpoint();
            endpoint.connect(address, 1000);
            // Post receive request
//		RdmaSendReceiveUtil.postReceiveReq(endpoint,++workRequestId);
            // TODO: give time for server to post Receive Work request RWR
            System.out.println("SimpleClient::client channel set up ");
            int i = 0;
            RdmaSendReceiveUtil.postReceiveReq(endpoint, ++workRequestId);
            RdmaMessage.PartitionRequest request = new RdmaMessage.PartitionRequest(i);
            request.writeTo(endpoint.getSendBuffer());
            RdmaSendReceiveUtil.postSendReq(endpoint, ++workRequestId);
            while (i <= 50) {
                long start = System.nanoTime();
                IbvWC wc = endpoint.getWcEvents().take();
                long end = System.nanoTime();
                System.out.println("Client Latency to pop-element out of queue " + (end - start));
                if (IbvWC.IbvWcOpcode.valueOf(wc.getOpcode()) == IbvWC.IbvWcOpcode.IBV_WC_RECV) {
                    i++;
                    if (wc.getStatus() != IbvWC.IbvWcStatus.IBV_WC_SUCCESS.ordinal()) {
                        System.out.println("Receive posting failed. reposting new receive request");
                        RdmaSendReceiveUtil.postReceiveReq(endpoint, ++workRequestId);
                    } else { // first receive succeeded. Read the data and repost the next message
                        RdmaMessage.PartitionResponse response = (RdmaMessage.PartitionResponse) RdmaMessage.PartitionResponse.readFrom(endpoint.getReceiveBuffer());
                        System.out.println("Response partition id: " + response.getPartitionId());
                        endpoint.getReceiveBuffer().clear();
                        RdmaSendReceiveUtil.postReceiveReq(endpoint, ++workRequestId);
                        RdmaMessage.PartitionRequest request1 = new RdmaMessage.PartitionRequest(i);
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
//            Thread.sleep(1000);
        }
        this.shutdown();
//		System.exit(0);
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        org.apache.log4j.Logger.getRootLogger().setLevel(Level.INFO);
        CmdLineCommon cmdLine = new CmdLineCommon("RdmaClient");
        try {
            cmdLine.parse(args);
        } catch (org.apache.commons.cli.ParseException e) {
            cmdLine.printHelp();
            System.exit(-1);
        }
        RdmaConfig rdmaConfig = new RdmaConfig(InetAddress.getByName(cmdLine.getIp()), cmdLine.getPort());
        RdmaClient client = new RdmaClient(rdmaConfig); // TODO: need to pass client partition handler
        client.run();
    }

    public void shutdown() {
        try {
            System.out.println("client endpoint closed");
            System.out.println("server endpoint closed");
            endpointGroup.close();
        } catch (Exception e) {
//            LOG.error(e.getMessage());
            e.printStackTrace();
        }
    }
}