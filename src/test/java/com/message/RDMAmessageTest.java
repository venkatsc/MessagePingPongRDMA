package com.message;

import java.nio.ByteBuffer;

public class RDMAmessageTest {
    public static void main(String[] args) {

        PartitionRequest request = new PartitionRequest(1);
        try {
            request.writeTo(ByteBuffer.allocateDirect(8));
            System.out.println(request.getPartitionId());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}

class PartitionRequest extends RdmaMessage {
    public final static int MESSAGE_LENGTH = /*ID*/ 1 + 4 /*partitionId*/;
    private int partitionId;
    private static final byte ID = 2;

    public PartitionRequest(int partitionId) {
        this.partitionId = partitionId;
    }

    public int getPartitionId() {
        return partitionId;
    }

    void writeTo(ByteBuffer buffer) throws Exception {
        if (buffer.capacity() < PartitionRequest.MESSAGE_LENGTH) {
            throw new Exception("Insufficient buffer capacity to write. Actual " + buffer.capacity() + " required" +
                    " " + PartitionRequest.MESSAGE_LENGTH);
        }
        buffer.put(ID);
        buffer.putInt(partitionId);
    }

    static RdmaMessage readFrom(ByteBuffer buffer) throws Exception {
        if (buffer.capacity() < PartitionRequest.MESSAGE_LENGTH) {
            throw new Exception("Insufficient buffer capacity to read. Actual " + buffer.capacity() + " required " +
                    "" + PartitionRequest.MESSAGE_LENGTH);
        }
        System.out.println("Message type id "+(int)buffer.get());
        PartitionRequest request = new PartitionRequest(buffer.getInt());

        return request;
    }
}

