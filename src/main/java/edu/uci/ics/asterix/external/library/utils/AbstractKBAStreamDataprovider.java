package edu.uci.ics.asterix.external.library.utils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
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

public abstract class AbstractKBAStreamDataprovider implements Runnable {
    protected static final int BUFFER_LENGTH = 8 * 1024;
    protected LinkedBlockingQueue<String> inputQueue;
    protected File[] dirDateHourDirs;
    protected static final Logger LOGGER = Logger.getLogger(AbstractKBAStreamDataprovider.class.getName());

    public AbstractKBAStreamDataprovider(LinkedBlockingQueue<String> inputQ, String localDir) {
        this.inputQueue = inputQ;
        dirDateHourDirs = KBACorpusFiles.getDateHourDirs(localDir);
    }

    private void readStremItem() throws TException, InterruptedException, IOException {

        for (File datehourdir : dirDateHourDirs) {

            File xz_files[] = KBACorpusFiles.getXZFiles(datehourdir);

            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Processing files in " + datehourdir.getName());
            }

            // Reading the thrift-based (by default, xz-compressed) stream files
            for (File file : xz_files) {
                FileInputStream fis = new FileInputStream(file);
                BufferedInputStream bis = new BufferedInputStream(new XZInputStream(fis), BUFFER_LENGTH);
                final TTransport transport = new TIOStreamTransport(bis);
                final TBinaryProtocol protocol = new TBinaryProtocol(transport);
                transport.open();
                try {
                    while (true) {
                        final StreamItem item = new StreamItem();
                        item.read(protocol);
                        KBAStreamDocument kbadoc = new KBAStreamDocument(item, datehourdir.getName());
                        inputQueue.put(kbadoc.toAdmEquivalent());
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
        }
    }

    @Override
    public void run() {
        try {

            readStremItem();
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
