package com.message;

import com.ibm.disni.verbs.IbvMr;

import java.nio.ByteBuffer;

public class BufferProvider {
    private int bufferSize = 10;
    private ByteBuffer ClientSendBuffer = ByteBuffer.allocateDirect(bufferSize);
    private ByteBuffer ClientReceiveBuffer = ByteBuffer.allocateDirect(bufferSize);

    private ByteBuffer ServerSendBuffer = ByteBuffer.allocateDirect(bufferSize);
    private ByteBuffer ServerReceiveBuffer = ByteBuffer.allocateDirect(bufferSize);

    public ByteBuffer getClientSendBuffer() {
        return ClientSendBuffer;
    }

    public ByteBuffer getClientReceiveBuffer() {
        return ClientReceiveBuffer;
    }

    public ByteBuffer getServerSendBuffer() {
        return ServerSendBuffer;
    }

    public ByteBuffer getServerReceiveBuffer() {
        return ServerReceiveBuffer;
    }
}
