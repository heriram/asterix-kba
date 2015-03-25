package edu.uci.ics.asterix.external.library.utils;

import java.io.InputStream;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class FileTBinaryProtocol extends TBinaryProtocol {
    public static final FileTBinaryProtocol INSTANCE = new FileTBinaryProtocol();
    
    private FileTBinaryProtocol() {
        super(null);
    }
    
    public FileTBinaryProtocol(TTransport transport) {
        super(transport);
    }
    
    public void setTransport(TTransport transport) {
        this.trans_ = transport;
    }
    
    public void initializeFileTransport(FileTIOStreamTransport transport, InputStream is) {
        this.trans_ = transport;
        transport.setInputStream(is); 
        try {
            transport.open();
        } catch (TTransportException e) {
            e.printStackTrace();
        }
    }
    
    @Override
    public void reset() {
        super.reset();
        if (trans_!=null)
            trans_.close();
    }
    
}
