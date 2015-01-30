package edu.uci.ics.asterix.external.library.utils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.logging.Logger;

public class BufferedStreamWriter {
    private OutputStream os;
    private ByteBuffer outputBuffer = ByteBuffer.allocate(32 * 1024);

    public BufferedStreamWriter(OutputStream os) throws Exception {
        this.os = os;
        
    }
    
    private void flushBufferFrames(byte[] string_byte) throws IOException {
        int max_length = outputBuffer.limit();
        
        ByteBuffer tempBuffer = ByteBuffer.wrap(string_byte);
        int remaining = tempBuffer.capacity();
        byte dst[] = new byte[max_length];
        while (max_length<remaining) {
            tempBuffer.get(dst);
            outputBuffer.put(dst);
            flush();
            remaining = tempBuffer.remaining();
        }
        dst = new byte[remaining];
        tempBuffer.get(dst);
        outputBuffer.put(dst);
    }

    public void writeStreamADMString(String streamADMString) throws IOException {
        byte[] b = (streamADMString + "\n").getBytes();
        if (outputBuffer.position() + b.length > outputBuffer.limit()) {
            flush();
            
            if (b.length <= outputBuffer.limit()) {
                outputBuffer.put(b);
            } else 
                flushBufferFrames(b);
                
        } else {
            outputBuffer.put(b);
        }
    }

 
    private void flush() throws IOException {
        outputBuffer.flip();
        os.write(outputBuffer.array(), 0, outputBuffer.limit());
        outputBuffer.position(0);
        outputBuffer.limit(32 * 1024);
    }

    public void resetOs(OutputStream os) {
        this.os = os;
    }
}
