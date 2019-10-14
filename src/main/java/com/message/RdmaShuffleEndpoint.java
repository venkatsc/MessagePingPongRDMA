package com.message;

import com.ibm.disni.RdmaActiveEndpoint;
import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.verbs.IbvMr;
import com.ibm.disni.verbs.IbvWC;
import com.ibm.disni.verbs.RdmaCmId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;

public class RdmaShuffleEndpoint extends RdmaActiveEndpoint {
//    private static final Logger LOG = LoggerFactory.getLogger(RdmaShuffleClientEndpoint.class);

    private int bufferSize; // Todo: set default buffer size
//
//    public ByteBuffer getReceiveBuffer() {
//        return receiveBuffer;
//    }
//
    private IbvMr wholeAddressSpace;
    private ByteBuffer receiveBuffer; // Todo: add buffer manager with multiple buffers
//    private IbvMr registeredReceiveMemory; // Registered memory for the above buffer
//
    private ByteBuffer sendBuffer;
    private ByteBuffer sendBuffer1;
//    private IbvMr registeredSendMemory;
//
//    private ByteBuffer availableFreeReceiveBuffers;
//    private IbvMr availableFreeReceiveBuffersRegisteredMemory;
    private ArrayBlockingQueue<RdmaMessage> requestQueue = new ArrayBlockingQueue<>(1000);
    private ArrayBlockingQueue responseQueue = new ArrayBlockingQueue<>(1000);
//    private PartitionRequestClientHandler clientHandler;

    private ArrayBlockingQueue<IbvWC> wcEvents = new ArrayBlockingQueue<IbvWC>(1000);
    private static int workRequestId;

    public RdmaShuffleEndpoint(RdmaActiveEndpointGroup<? extends RdmaActiveEndpoint> group, RdmaCmId idPriv,
                                     boolean serverSide, int bufferSize)
            throws IOException {
        super(group, idPriv, serverSide);
        this.bufferSize = bufferSize; // Todo: validate buffer size
//        this.clientHandler = clientHandler;
    }

    @Override
    public void dispatchCqEvent(IbvWC wc) throws IOException {
        synchronized (this) {
            wcEvents.add(wc);
        }
    }

    public void init() throws IOException {
        super.init();
        this.sendBuffer = ByteBuffer.allocateDirect(bufferSize); // allocate buffer
        this.sendBuffer1 = ByteBuffer.allocateDirect(bufferSize);
        this.receiveBuffer = ByteBuffer.allocateDirect(2*bufferSize); // allocate buffer
        // Register on demand paging
        wholeAddressSpace = registerMemory().execute().getMr();

        System.out.printf("rkey: %d lkey: %d handle:%d\n",wholeAddressSpace.getRkey(),wholeAddressSpace.getLkey(),wholeAddressSpace.getHandle());

//        this.registeredReceiveMemory = registerMemory(receiveBuffer).execute().getMr(); // register the send buffer
//

//        this.registeredSendMemory = registerMemory(sendBuffer).execute().getMr(); // register the send buffer
//
//        this.availableFreeReceiveBuffers = ByteBuffer.allocateDirect(2); // TODO: assumption of less receive buffers
//        this.availableFreeReceiveBuffersRegisteredMemory = registerMemory(availableFreeReceiveBuffers).execute()
//                .getMr();
    }
//
    public ByteBuffer getReceiveBuffer() {
        return this.receiveBuffer;
    }

    public ByteBuffer getSendBuffer() {
        return this.sendBuffer;
    }

    public ByteBuffer getSendBuffer1() {
        return this.sendBuffer;
    }
//
    public IbvMr getMR() {
        return wholeAddressSpace;
    }
//
//    public IbvMr getRegisteredSendMemory() {
//        return registeredSendMemory;
//    }

    public ArrayBlockingQueue<IbvWC> getWcEvents() {
        synchronized (this) {
            return wcEvents;
        }
    }

//    public ByteBuffer getAvailableFreeReceiveBuffers() {
//        return availableFreeReceiveBuffers;
//    }
//
//    public IbvMr getAvailableFreeReceiveBuffersRegisteredMemory() {
//        return availableFreeReceiveBuffersRegisteredMemory;
//    }

//    public void write(RdmaMessage msg) {
//        // TODO (venkat):imp : pass buffer allocator
//        try {
//            msg.writeTo(this.getSendBuffer());
//            RdmaSendReceiveUtil.postSendReq(this, ++workRequestId);
//            IbvWC wcSend = this.getWcEvents().take();
//            if (wcSend.getStatus() != IbvWC.IbvWcStatus.IBV_WC_SUCCESS.ordinal()) {
//                System.out.println("failed to send the send request. status: " + wcSend.getStatus() + "WC type: "+wcSend.getOpcode());
//                // LOG the failure
//            }
//            this.sendBuffer.clear();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    public RdmaMessage read() {
//        try {
//            // (TODO: venkat) make sure the message type, for now we have only one message type
//            RdmaMessage response = RdmaMessage.PartitionResponse.readFrom(this.receiveBuffer);
//            this.receiveBuffer.clear();
//            return response;
//        } catch (Throwable e) {
//            e.printStackTrace();
//        }
//        return null;
//    }

    public void terminate() {
        try {
            this.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

