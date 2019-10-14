package com.message;

import java.nio.Buffer;
import java.nio.ByteBuffer;

public class BufferPutTest {

    public static void main(String[] args) {
        ByteBuffer direct = ByteBuffer.allocateDirect(39);
        ((ByteBuffer) direct).putInt(35,10);
        System.out.println(direct.getInt(35));
    }
}
