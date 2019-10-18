package com.message;

import java.nio.ByteBuffer;

public abstract class RdmaMessage {


    abstract void writeTo(ByteBuffer buffer) throws Exception;
//    abstract void readFrom(ByteBuffer buffer) throws Exception;
//    abstract int messageLength();
     public static final int MAGIC_NUMBER = 0xBADC0FFE;
    static class PartitionRequest extends RdmaMessage {
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
            buffer.putInt(5);
            buffer.putInt(MAGIC_NUMBER);
            buffer.put(ID);
            buffer.putInt(partitionId);
        }

        static RdmaMessage readFrom(ByteBuffer buffer) throws Exception {
            if (buffer.capacity() < PartitionRequest.MESSAGE_LENGTH) {
                throw new Exception("Insufficient buffer capacity to read. Actual " + buffer.capacity() + " required " +
                        "" + PartitionRequest.MESSAGE_LENGTH);
            }
//            System.out.println("Request message type id "+(int)buffer.get());
            validateMagice(buffer.getInt());
            buffer.get(); //discard ID
            PartitionRequest request = new PartitionRequest(buffer.getInt());
            return request;
        }
    }

    private static void validateMagice(int magic) throws Exception {
        if (magic!=RdmaMessage.MAGIC_NUMBER){
            throw new Exception("Invalid magic number "+magic);
        }
    }

    static class PartitionResponse extends RdmaMessage {

        private int partitionId;
        private static final byte ID = 3;
        private static String message = "here we go";
        public final static int MESSAGE_LENGTH = /*ID*/ 4 + 4 /*partitionId*/;// + message.length() * 2;

        public int getPartitionId() {
            return partitionId;
        }

        public PartitionResponse(int partitionId) {
            this.partitionId = partitionId;
        }

        void writeTo(ByteBuffer buffer) throws Exception {
            if (buffer.capacity() < PartitionResponse.MESSAGE_LENGTH) {
                throw new Exception("Insufficient buffer capacity to write. Actual " + buffer.capacity() + " required" +
                        " " + PartitionResponse.MESSAGE_LENGTH);
            }
            buffer.putInt(5);
            buffer.putInt(MAGIC_NUMBER);
            buffer.put(ID);
            buffer.putInt(partitionId);
//            buffer.asCharBuffer().put(message);
        }

        static RdmaMessage readFrom(ByteBuffer buffer) throws Exception {
            if (buffer.capacity() < PartitionResponse.MESSAGE_LENGTH) {
                throw new Exception("Insufficient buffer capacity to read. Actual " + buffer.capacity() + " required " +
                        "" + PartitionResponse.MESSAGE_LENGTH);
            }
            validateMagice(buffer.getInt());
            buffer.get();
//            System.out.println("Response message id "+(int)buffer.get());
            PartitionResponse response = new PartitionResponse(buffer.getInt());
//            byte[] message = new byte[20];
//            System.out.println(buffer.capacity());
//            System.out.println(buffer.get(message, 9, 30).toString());
            return response;
        }

    }

}
