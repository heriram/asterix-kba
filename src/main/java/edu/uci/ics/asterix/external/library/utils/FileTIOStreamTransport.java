package edu.uci.ics.asterix.external.library.utils;

import java.io.InputStream;

import org.apache.thrift.transport.TIOStreamTransport;

public class FileTIOStreamTransport extends TIOStreamTransport {
    public static FileTIOStreamTransport INSTANCE = new FileTIOStreamTransport();
    private FileTIOStreamTransport() {
        super();
    }
    
    public void setInputStream(InputStream is) {
        this.inputStream_ = is;
    }
    
}