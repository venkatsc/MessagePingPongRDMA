package com.message;

import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.RdmaEndpointFactory;
import com.ibm.disni.RdmaServerEndpoint;
import com.ibm.disni.verbs.IbvContext;
import com.ibm.disni.verbs.IbvWC;
import com.ibm.disni.verbs.RdmaCmId;
import org.apache.commons.cli.ParseException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

public class RdmaServer implements RdmaEndpointFactory<RdmaShuffleServerEndpoint> {
    //private static final Logger LOG = LoggerFactory.getLogger(RdmaServer.class);
    private RdmaActiveEndpointGroup<RdmaShuffleServerEndpoint> endpointGroup;
    private final RdmaConfig rdmaConfig;
    private int workRequestId = 1;
    private RdmaServerEndpoint<RdmaShuffleServerEndpoint> serverEndpoint;
    private InetSocketAddress address;

    public RdmaShuffleServerEndpoint getClientEndpoint() {
        return clientEndpoint;
    }

    private RdmaShuffleServerEndpoint clientEndpoint;
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

    public RdmaServer(RdmaConfig rdmaConfig) {
        this.rdmaConfig = rdmaConfig;
    }

    public void run() throws Exception {
        //create a EndpointGroup. The RdmaActiveEndpointGroup contains CQ processing and delivers CQ event to the
        // endpoint.dispatchCqEvent() method.
        endpointGroup = new RdmaActiveEndpointGroup<RdmaShuffleServerEndpoint>(1000, true, 128, 4, 128,false);
        endpointGroup.init(this);
        //create a server endpoint
        serverEndpoint = endpointGroup.createServerEndpoint();

        //we can call bind on a server endpoint, just like we do with sockets
//		InetAddress ipAddress = InetAddress.getByName(host);
        address = new InetSocketAddress(rdmaConfig.getServerAddress(), rdmaConfig.getServerPort());
        try {
            serverEndpoint.bind(address, 10);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("SimpleServer::servers bound to address " + address.toString());
        int conns=0;
        while(conns<2) {
            //we can accept new connections
            clientEndpoint = serverEndpoint.accept();
            conns++;
            //we have previously passed our own endpoint factory to the group, therefore new endpoints will be of type
            // CustomServerEndpoint
//		System.out.println("SimpleServer::client connection accepted");
//
//        RdmaSendReceiveUtil.postReceiveReq(clientEndpoint, ++workRequestId);
            int i = 0;
            String message;

//        while (i <= 50) {
//            i++;
//            RdmaMessage.PartitionRequest request = (RdmaMessage.PartitionRequest) clientEndpoint.read();
//            System.out.println(" Partition requested "+request.getPartitionId());
//            RdmaMessage.PartitionResponse response = new RdmaMessage.PartitionResponse(i);
//            clientEndpoint.write(response);
//            if (request.getPartitionId() == 50) {
//                break;
//            }
//        }
            RdmaSendReceiveUtil.postReceiveReq(clientEndpoint, ++workRequestId);
            while (i <= 50) {
                long start = System.nanoTime();
                IbvWC wc = clientEndpoint.getWcEvents().take();
                long end = System.nanoTime();
                System.out.println("Server Latency to pop-element out of queue "+ (end-start));
                if (IbvWC.IbvWcOpcode.valueOf(wc.getOpcode()) == IbvWC.IbvWcOpcode.IBV_WC_RECV) {
                    i++;
                    if (wc.getStatus() != IbvWC.IbvWcStatus.IBV_WC_SUCCESS.ordinal()) {
                        System.out.println("Receive posting failed. reposting new receive request");
                        RdmaSendReceiveUtil.postReceiveReq(clientEndpoint, ++workRequestId);
                    } else { // first receive succeeded. Read the data and repost the next message
                        RdmaMessage.PartitionRequest clientRequest = (RdmaMessage.PartitionRequest) RdmaMessage.PartitionRequest.readFrom(clientEndpoint.getReceiveBuffer());

                        System.out.println("client requested partition id: " + clientRequest.getPartitionId());
                        clientEndpoint.getReceiveBuffer().clear();
                        RdmaMessage.PartitionRequest request = new RdmaMessage.PartitionRequest(clientRequest
                                .getPartitionId());
                        request.writeTo(clientEndpoint.getSendBuffer());
                        RdmaSendReceiveUtil.postReceiveReq(clientEndpoint, ++workRequestId); // post next receive
                        RdmaSendReceiveUtil.postSendReq(clientEndpoint, ++workRequestId);
                    }
                } else if (IbvWC.IbvWcOpcode.valueOf(wc.getOpcode()) == IbvWC.IbvWcOpcode.IBV_WC_SEND) {
                    if (wc.getStatus() != IbvWC.IbvWcStatus.IBV_WC_SUCCESS.ordinal()) {
                        System.out.println("Send failed. reposting new send request request");
                        RdmaSendReceiveUtil.postSendReq(clientEndpoint, ++workRequestId);
                    }
                    clientEndpoint.getSendBuffer().clear();
                    // Send succeed does not require any action
                } else {
                    System.out.println("failed to match any condition " + wc.getOpcode());
                }
            }
        }
        //close everything
//        System.out.println("group closed");
//        this.shutdown();
//		System.exit(0);
    }

    public static void main(String[] args) throws Exception {
        CmdLineCommon cmdLine = new CmdLineCommon("RdmaServer");
        try {
            cmdLine.parse(args);
        } catch (ParseException e) {
            cmdLine.printHelp();
            System.exit(-1);
        }
        RdmaConfig rdmaConfig = new RdmaConfig(InetAddress.getByName(cmdLine.getIp()), cmdLine.getPort());
        RdmaServer server = new RdmaServer(rdmaConfig); //TODO (venkat): it should not
        // be null
        server.run();
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
