package com.message;

import com.ibm.disni.verbs.IbvMr;

import java.nio.ByteBuffer;

public class BufferProvider {
    private int bufferSize = 25;
    private ByteBuffer ClientSendBuffer = ByteBuffer.allocateDirect(bufferSize);
    private ByteBuffer ClientReceiveBuffer = ByteBuffer.allocateDirect(bufferSize);

    private ByteBuffer ServerSendBuffer = ByteBuffer.allocateDirect(bufferSize);
    private ByteBuffer ServerReceiveBuffer = ByteBuffer.allocateDirect(bufferSize);

    private IbvMr registeredClientReceiveMemory; // Registered memory for the above buffer
    private IbvMr registeredClientSendMemory;

    public IbvMr getRegisteredClientReceiveMemory() {
        return registeredClientReceiveMemory;
    }

    public void setRegisteredClientReceiveMemory(IbvMr registeredClientReceiveMemory) {
        this.registeredClientReceiveMemory = registeredClientReceiveMemory;
    }

    public IbvMr getRegisteredClientSendMemory() {
        return registeredClientSendMemory;
    }

    public void setRegisteredClientSendMemory(IbvMr registeredClientSendMemory) {
        this.registeredClientSendMemory = registeredClientSendMemory;
    }

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
