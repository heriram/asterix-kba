package edu.uci.ics.asterix.external.library.utils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.trec.kba.streamcorpus.StreamItem;
import org.tukaani.xz.XZInputStream;

public abstract class AbstractKBAStreamDataprovider<E> implements Runnable {
    protected static final int BUFFER_LENGTH = 8 * 1024;
    protected LinkedBlockingQueue<E> inputQueue;
    protected File[] dirDateHourDirs;
    protected static final Logger LOGGER = Logger.getLogger(AbstractKBAStreamDataprovider.class.getName());
    
    protected static final boolean ADM_STRING = true;
    protected static final boolean STREAM_ITEM = false;
    private boolean isAdmString = ADM_STRING;
    
    protected boolean continueReading = true;
    
    public AbstractKBAStreamDataprovider(LinkedBlockingQueue<E> inputQ, String localDir) {
        this.inputQueue = inputQ;
        dirDateHourDirs = KBACorpusFiles.getDateHourDirs(localDir);
    }

    public static class KBAStreamItem extends StreamItem {

        private static final long serialVersionUID = 1L;

        String dirName;
        public KBAStreamItem(String dirName) {
            super();
            this.dirName = dirName;
        }
        
        public String getDirName() {
            return this.dirName;
        }
        
    }
    
    public void setElementType(boolean isAdmString) {
        this.isAdmString = isAdmString;
    }
    
    public void readChunk(File file, String dirName) throws InterruptedException, TException, IOException {
        FileInputStream fis = new FileInputStream(file);
        BufferedInputStream bis = new BufferedInputStream(new XZInputStream(fis), BUFFER_LENGTH);
        final TTransport transport = new TIOStreamTransport(bis);
        final TBinaryProtocol protocol = new TBinaryProtocol(transport);
        transport.open();
        try {
            while (true) {
                final StreamItem item = new KBAStreamItem(dirName);
                item.read(protocol);
                if (isAdmString) {
                    KBAStreamDocument kbadoc = new KBAStreamDocument(item, dirName);
                    inputQueue.put((E) kbadoc.toAdmEquivalent());
                } else 
                    inputQueue.put((E) item);
                
                Thread.sleep(1);
            }

        } catch (TTransportException te) {
            // Deal with the EOF exception bug
            if (te.getType() == TTransportException.END_OF_FILE
                    || te.getCause() instanceof java.io.EOFException) {
                ; // Do nothing:-)
            } else {
                throw te;
            }
        }
        transport.close();
        fis.close();
        
    }
    
    private void readAllItems() throws TException, InterruptedException, IOException {
        
        for (int j=0; j<dirDateHourDirs.length; j++ ) {
            File xz_files[] = KBACorpusFiles.getXZFiles(dirDateHourDirs[j]);

            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Processing files in " + dirDateHourDirs[j].getName());
            }

            // Reading the thrift-based (by default, xz-compressed) stream files
            int i = 0;
            for (; i<xz_files.length && continueReading; i++) {
                File file = xz_files[i];
                readChunk(file,dirDateHourDirs[j].getName());
            }
        }
    }

    @Override
    public void run() {
        try {

            readAllItems();
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE,
                    "Fileloading/reading exception when trying to read the stream item. " + e.getMessage());
            e.printStackTrace();

        } catch (TException e) {
            LOGGER.log(Level.WARNING, "Thrift Exception when trying to read the stream item. " + e.getMessage());
        } catch (InterruptedException e) {
            LOGGER.log(Level.WARNING, "Interruption Exception when trying to put an element in the inputQueue. "
                    + e.getMessage());
        }
    }

    public abstract void stop();

}
