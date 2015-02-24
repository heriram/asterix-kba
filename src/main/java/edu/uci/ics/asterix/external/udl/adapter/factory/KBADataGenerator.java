package edu.uci.ics.asterix.external.udl.adapter.factory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.trec.kba.streamcorpus.StreamItem;
import org.tukaani.xz.XZInputStream;

import edu.uci.ics.asterix.external.library.utils.KBACorpusFiles;
import edu.uci.ics.asterix.external.library.utils.KBAStreamDocument;

/**
 * Generate data stream based on the KBA streams for Asterix
 * 
 * @author Heri Ramampiaro <heri@ntnu.no>
 */
public class KBADataGenerator {
    public static final int BUFFER_LENGTH_8KB = 8 * 1024;
    public static final int BUFFER_LENGTH_4KB = 4 * 1024;
    public static final int BUFFER_LENGTH_2KB = 2 * 1024;

    private static final Logger LOGGER = Logger.getLogger(KBADataGenerator.class.getName());

    private KBAStreamDocReader kbaReader = null;

    private static File[] dirDateHourDirs;

    public KBADataGenerator(int init_capacity) {
        dirDateHourDirs = KBACorpusFiles.getDateHourDirs();
        kbaReader = new KBAStreamDocReader(init_capacity);
    }

    public KBADataGenerator(String corpusdir, int init_capacity) {
        dirDateHourDirs = KBACorpusFiles.getDateHourDirs(corpusdir);
        kbaReader = new KBAStreamDocReader(init_capacity);
    }

    public KBAStreamDocIterator getIterator() {
        return new KBAStreamDocIterator();
    }

    /**
     * Iterates over the KBA Stream Documents
     */
    public class KBAStreamDocIterator implements Iterator<KBAStreamDocument> {

        Iterator<KBAStreamDocument> streamItemListIterator;

        public KBAStreamDocIterator() {
            List<KBAStreamDocument> stream_item_list = kbaReader.getStreamItemList();
            streamItemListIterator = stream_item_list.iterator();
        }

        @Override
        public boolean hasNext() {
            if (streamItemListIterator.hasNext())
                return true;

            // If the current list is empty, then get more data
            if (kbaReader.hasMoreToRead()) {
                try {
                    kbaReader.getMoreStreamItems();
                    streamItemListIterator = kbaReader.getStreamItemList().iterator();

                } catch (TException e) {
                    LOGGER.log(Level.SEVERE, "Could not read streams from " + kbaReader.getClass().getName());
                    e.printStackTrace();
                }
                return true;

            } else
                return false;
        }

        @Override
        public KBAStreamDocument next() {                   
            return streamItemListIterator.next();
        }

    }

    public static class KBAStreamDocReader {
        private List<KBAStreamDocument> streamItems;

        public static final int STREAM_BUFFER_SIZE = 10000;
        private int streamBufferSize = STREAM_BUFFER_SIZE;
        private int currentBatchNumber = 1;
        
        private static boolean moreDateHourToRead = false;
        private static int filePosition = 0;
        private static int dateHourPosition = 0;


        public KBAStreamDocReader() {
            try {
                read();
            } catch (TException e) {
                e.printStackTrace();
            }
        }

        public KBAStreamDocReader(int stream_buffer_size) {
            this.streamBufferSize = stream_buffer_size;
            try {
                read();
            } catch (TException e) {
                e.printStackTrace();
            }

        }

        public void getMoreStreamItems() throws TException {
            this.streamItems.clear();
            read();
        }

        public boolean hasMoreToRead() {
            return moreDateHourToRead;
        }

        private InputStream getInputStream(File in_file) {
            try {
                FileInputStream fis = new FileInputStream(in_file);
                BufferedInputStream file = new BufferedInputStream(new XZInputStream(fis), BUFFER_LENGTH_8KB);
                return file;

            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }



        public void read() throws TException {
            streamItems = new ArrayList<KBAStreamDocument>(this.streamBufferSize);
            boolean stop_next = false;

            int dh_index = dateHourPosition;

            for (; !stop_next && dh_index < dirDateHourDirs.length; dh_index++) {
                File datehourdir = dirDateHourDirs[dh_index];
                File xz_files[] = KBACorpusFiles.getXZFiles(datehourdir);
                
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Processing batch nr. " + (currentBatchNumber) + "  for " + dirDateHourDirs[dh_index].getName());
                    LOGGER.info("Starting at " + (filePosition) + " of "+ xz_files.length + " file index in " + dirDateHourDirs[dh_index].getName());
                }

                // Reading the thrift-based (by default, xz-compressed) stream files
                int file_index = filePosition;
                for (; file_index < xz_files.length; file_index++) {
                    final File file = xz_files[file_index];
                    final TTransport transport = new TIOStreamTransport(getInputStream(file));
                    final TBinaryProtocol protocol = new TBinaryProtocol(transport);
                    transport.open();

                    try {
                        while (true) {
                            final StreamItem item = new StreamItem();
                            item.read(protocol);

                            KBAStreamDocument kba_stream = new KBAStreamDocument(item, datehourdir.getName());
                            streamItems.add(kba_stream);
                            stop_next = streamItems.size() >= streamBufferSize;
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
                    if (stop_next) {
                        filePosition = ++file_index;
                        dateHourPosition = dh_index;
                        currentBatchNumber++;
                        break;
                    }
                }
                if (stop_next)
                    break;

                filePosition = 0;
                currentBatchNumber = 1;
            }
            moreDateHourToRead = (dh_index < dirDateHourDirs.length);
        }

        public List<KBAStreamDocument> getStreamItemList() {
            return streamItems;
        }

        public boolean hasMoreDateHourToRead() {
            return moreDateHourToRead;
        }

        public int getCapacity() {
            return this.streamBufferSize;
        }

    }

}