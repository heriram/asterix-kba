package edu.uci.ics.asterix.external.library.utils;

import java.io.InputStream;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TType;
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

    public void closeTransport() {
        if (trans_ != null && trans_.isOpen())
            trans_.close();
    }

    public void openTransport() {
        if (trans_ != null && !trans_.isOpen()) {
            try {
                trans_.open();
            } catch (TTransportException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public TField readFieldBegin() throws TException {
        TField tField = null;
        
        byte type = readByte();
        short id = type == TType.STOP ? 0 : readI16();
        tField = new TField("", type, id);
        
        return tField;
    }

    @Override
    public void reset() {
        super.reset();
        if (trans_ != null)
            trans_.close();
    }

}
