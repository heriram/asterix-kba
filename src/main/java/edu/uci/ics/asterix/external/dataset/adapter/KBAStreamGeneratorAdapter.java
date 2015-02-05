/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.external.dataset.adapter;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.SequenceInputStream;
import java.util.Enumeration;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.trec.kba.streamcorpus.StreamItem;
import org.tukaani.xz.XZInputStream;

import edu.uci.ics.asterix.common.feeds.api.IFeedAdapter;
import edu.uci.ics.asterix.external.dataset.adapter.StreamBasedAdapter;
import edu.uci.ics.asterix.external.library.utils.KBACorpusFiles;
import edu.uci.ics.asterix.external.library.utils.KBAStreamDocument;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.runtime.operators.file.AsterixTupleParserFactory;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

/**
 * Adapter class for creating @see{KBAStreamGeneratorAdapter}.
 * The adapter reads streams of KBA streams from Thrift, and "push" them into Asterix.
 * 
 * @author heri
 */

public class KBAStreamGeneratorAdapter extends StreamBasedAdapter implements IFeedAdapter {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = Logger.getLogger(KBAStreamGeneratorAdapter.class.getName());

    

    private FileContentProvider contentProvider;

    //private final KBAStreamServer streamDocServer;

    public KBAStreamGeneratorAdapter(Map<String, String> configuration, ITupleParserFactory parserFactory,
            ARecordType outputtype, int partition, IHyracksTaskContext ctx) throws Exception {
        super(parserFactory, outputtype, ctx, partition);
        //this.streamDocServer = new KBAStreamServer(configuration, partition, outputtype, outputStream, executorService);
        String dirSplits[] = configuration.get(AsterixTupleParserFactory.KEY_PATH).split(",");
        this.contentProvider = new FileContentProvider(dirSplits, ctx.getFrameSize());
        
    }

    @Override
    public InputStream getInputStream(int partition) throws IOException {
        //streamDocServer.start();
        return contentProvider.getInputStream(partition);
        //return inputStream;
    }
    
    
    private static class FileContentProvider {
        private String[] directorySplits;
        private int maxTupleSize=Integer.MAX_VALUE;

        public FileContentProvider(String [] dir_split, int frameSize) {
            this.directorySplits = dir_split;
            this.maxTupleSize = (int)(0.80*frameSize)/2;
        }

        private InputStream readChunk(File inputfile, String dirname) throws TException, InterruptedException,
                IOException {
            FileInputStream fis = new FileInputStream(inputfile);
            BufferedInputStream bis = new BufferedInputStream(new XZInputStream(fis), (32 * 1024));
            final TTransport transport = new TIOStreamTransport(bis);
            final TBinaryProtocol protocol = new TBinaryProtocol(transport);
            transport.open();
            StringBuilder sb = new StringBuilder();
            try {
                while (true) {
                    final StreamItem item = new StreamItem();
                    item.read(protocol);
                    KBAStreamDocument kbadoc = new KBAStreamDocument(item, dirname);
                    String admString = kbadoc.toAdmEquivalent();
                    
                    int size = admString.getBytes().length;
                    if (size>maxTupleSize)
                        LOGGER.log(Level.INFO, "Reccord too long?: "+ size + " b. Could be Discarded.");
                    
                    sb.append(admString).append('\n');
                }

            } catch (TTransportException te) {
                // Deal with the EOF exception bug
                if (te.getType() == TTransportException.END_OF_FILE || te.getCause() instanceof java.io.EOFException) {
                    ; // Do nothing:-)
                } else {
                    throw te;
                }
            }
            transport.close();
            fis.close();

            return new ByteArrayInputStream(sb.toString().getBytes());
        }

        /**
         * Perform a sequential read of the current file list
         * 
         * @param partition
         *            partition number of the directory
         * @return {@link InputStream} for the file contents
         * @throws IOException
         */
        public InputStream getInputStream(int partition) throws IOException {
            //final File directory = directorySplits[partition].getLocalFile().getFile();
            final File directory = new File(directorySplits[partition]);
            final File[] files = KBACorpusFiles.getXZFiles(directory);

            if (files == null)
                throw new IOException("cannot get contents from an empty file list.");

            Enumeration<InputStream> inputstream_enumeration = new Enumeration<InputStream>() {
                int index;

                @Override
                public boolean hasMoreElements() {
                    return index < files.length;
                }

                @Override
                public InputStream nextElement() {
                    index++;
                    try {
                        File file = files[index - 1];

                        return readChunk(file, directory.getName());

                    } catch (Exception ex) {
                        throw new RuntimeException("Error getting the stream content", ex);
                    }
                }
            };
            return new SequenceInputStream(inputstream_enumeration);

        }

    }


   /* private static class KBAStreamServer {
        private final DataProvider dataProvider;
        private final ExecutorService executorService;

        public KBAStreamServer(Map<String, String> configuration, int partition, ARecordType outputtype,
                OutputStream os, ExecutorService executorService) throws Exception {
            this.dataProvider = new DataProvider(configuration, outputtype, partition, os);
            this.executorService = executorService;
        }

        public void stop() throws IOException {
            dataProvider.stop();
            executorService.shutdown();
        }

        public void start() {
            executorService.execute(dataProvider);
        }

    }

    private static class DataProvider implements Runnable {

        public static final String KEY_BATCHSIZE = KBAStreamFeeder.KEY_BATCHIZE;

        private KBAStreamFeeder kbaDataGenerator;
        private boolean continuePush = true;
        private int batchSize = 5000;;
        private final OutputStream os;

        public DataProvider(Map<String, String> configuration, ARecordType outputtype, int partition, OutputStream os)
                throws Exception {
            batchSize = Integer.parseInt(configuration.get(KEY_BATCHSIZE));
            this.kbaDataGenerator = new KBAStreamFeeder(configuration, partition, os);
            this.os = os;
        }

        @Override
        public void run() {
            boolean moreData = true;

            while (true) {
                try {
                    while (moreData && continuePush) {
                        moreData = kbaDataGenerator.setNextRecordBatch(batchSize);
                    }
                    os.close();
                    break;
                } catch (Exception e) {
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning("Exception in adaptor " + e.getMessage());
                    }
                }
            }
        }

        public void stop() {
            continuePush = false;
        }

    }*/

   /* @Override
    public void stop() throws Exception {
        streamDocServer.stop();
    }*/
 
    @Override
    public DataExchangeMode getDataExchangeMode() {
        return DataExchangeMode.PUSH;
    }

    @Override
    public boolean handleException(Exception e) {
        /*try {
            streamDocServer.stop();
        } catch (Exception re) {
            re.printStackTrace();
            return false;
        }
        streamDocServer.start();*/
        return true;
    }

    @Override
    public void stop() throws Exception {
        // TODO Auto-generated method stub
        LOGGER.info(KBAStreamGeneratorAdapter.class.getName()+ " stopped successfully.");
    }
}