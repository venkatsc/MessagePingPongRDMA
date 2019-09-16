/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.message;

import com.ibm.disni.RdmaActiveEndpoint;
import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.verbs.IbvMr;
import com.ibm.disni.verbs.IbvWC;
import com.ibm.disni.verbs.RdmaCmId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;


public class RdmaShuffleServerEndpoint extends RdmaActiveEndpoint {
    private int workRequestId = 0;
    private int bufferSize; // Todo: set default buffer size
    private ByteBuffer sendBuffer; // Todo: add buffer manager with multiple buffers
//    private IbvMr registeredSendMemory; // Registered memory for the above buffer
//
//    private ByteBuffer availableFreeReceiveBuffers;
//    private IbvMr availableFreeReceiveBuffersRegisteredMemory;
//
//    private ByteBuffer availableFreeReceiveBuffersNotification;
//    private IbvMr availableFreeReceiveBuffersNotificationRegisteredMemory;
//
    private ByteBuffer receiveBuffer; // Todo: add buffer manager with multiple buffers
//    private IbvMr registeredReceiveMemory;

    private IbvMr wholeAddressSpace;
    private ArrayBlockingQueue<IbvWC> wcEvents = new ArrayBlockingQueue<>(1000);

    public RdmaShuffleServerEndpoint(RdmaActiveEndpointGroup<? extends RdmaActiveEndpoint> group, RdmaCmId idPriv,
                                     boolean serverSide, int bufferSize) throws IOException {
        super(group, idPriv, serverSide);
        this.bufferSize = bufferSize; // Todo: validate buffer size
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
        this.receiveBuffer = ByteBuffer.allocateDirect(bufferSize);
        this.wholeAddressSpace = registerMemory().execute().getMr();
        System.out.printf("rkey: %d lkey: %d handle:%d\n",wholeAddressSpace.getRkey(),wholeAddressSpace.getLkey(),wholeAddressSpace.getHandle());
//        registerMemory()
//        this.registeredReceiveMemory = registerMemory(receiveBuffer).execute().getMr();
//        this.registeredSendMemory = registerMemory(sendBuffer).execute().getMr(); // register the send buffer
//        this.availableFreeReceiveBuffers = ByteBuffer.allocateDirect(2); // TODO: assumption of less receive buffers
//        this.availableFreeReceiveBuffersRegisteredMemory = registerMemory(availableFreeReceiveBuffers).execute()
//                .getMr();
//        this.availableFreeReceiveBuffersNotification = ByteBuffer.allocateDirect(2); // TODO: assumption of less
//        // receive buffers
//        this.availableFreeReceiveBuffersNotificationRegisteredMemory = registerMemory
//                (availableFreeReceiveBuffersNotification).execute().getMr();
    }
    public IbvMr getMR() {
        return wholeAddressSpace;
    }
    public ByteBuffer getSendBuffer() {
        return this.sendBuffer;
    }
//
//    public IbvMr getRegisteredSendMemory() {
//        return registeredSendMemory;
//    }
//
    public ByteBuffer getReceiveBuffer() {
        return this.receiveBuffer;
    }
//
//    public IbvMr getRegisteredReceiveMemory() {
//        return registeredReceiveMemory;
//    }

    public ArrayBlockingQueue<IbvWC> getWcEvents() {
        synchronized (this) {
            return wcEvents;
        }
    }
//
//    public ByteBuffer getAvailableFreeReceiveBuffers() {
//        return availableFreeReceiveBuffers;
//    }
//
//    public IbvMr getAvailableFreeReceiveBuffersRegisteredMemory() {
//        return availableFreeReceiveBuffersRegisteredMemory;
//    }
//
//    public ByteBuffer getAvailableFreeReceiveBuffersNotification() {
//        return availableFreeReceiveBuffersNotification;
//    }
//
//    public IbvMr getAvailableFreeReceiveBuffersNotificationRegisteredMemory() {
//        return availableFreeReceiveBuffersNotificationRegisteredMemory;
//    }

    // Write response {RdmaMessage.PartitionResponse}
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
//            RdmaSendReceiveUtil.postReceiveReq(this, ++workRequestId);
//            IbvWC wc = this.getWcEvents().take();
//            if (wc.getStatus() != IbvWC.IbvWcStatus.IBV_WC_SUCCESS.ordinal()) {
//                System.out.println("failed to send the receive request. status: " + wc.getStatus() + "WC type: "+wc.getOpcode());
//                // LOG the failure
//            }
//            // (TODO: venkat) make sure the message type, for now we have only one message type
//            RdmaMessage response = RdmaMessage.PartitionRequest.readFrom(this.receiveBuffer);
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
