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
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.SequenceInputStream;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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

import edu.uci.ics.asterix.common.feeds.api.IFeedAdapter;
import edu.uci.ics.asterix.external.library.utils.BufferedStreamWriter;
import edu.uci.ics.asterix.external.library.utils.KBACorpusFiles;
import edu.uci.ics.asterix.external.library.utils.KBAStreamDocument;
import edu.uci.ics.asterix.external.library.utils.KBAStreamDocumentWriter;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.runtime.operators.file.AsterixTupleParserFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
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

    private ExecutorService executorService = Executors.newCachedThreadPool();

    private PipedOutputStream outputStream = new PipedOutputStream();

    private PipedInputStream inputStream = new PipedInputStream(outputStream);

    private LinkedBlockingQueue<File> dateHourDirectoryList;

    private final KBAStreamServer streamDocServer;

    public KBAStreamGeneratorAdapter(Map<String, String> configuration, ITupleParserFactory parserFactory,
            ARecordType outputtype, int partition, IHyracksTaskContext ctx) throws Exception {
        super(parserFactory, outputtype, ctx, partition);
        configureDateHourDirectories(configuration, partition);
        this.streamDocServer = new KBAStreamServer(dateHourDirectoryList, partition, outputtype, ctx.getFrameSize(),
                outputStream, executorService);
    }

    @Override
    public InputStream getInputStream(int partition) throws IOException {
        streamDocServer.start();
        return inputStream;
    }

    private void configureDateHourDirectories(Map<String, String> configuration, int partition) {
        String corpusDir = configuration.get(AsterixTupleParserFactory.KEY_PATH).split(",")[partition];
        File dateHourDirs[] = KBACorpusFiles.getDateHourDirs(corpusDir);
        this.dateHourDirectoryList = new LinkedBlockingQueue<File>(Arrays.asList(dateHourDirs));
    }

    private static class FileContentProvider {
        private String[] directorySplits;
        private int maxTupleSize = Integer.MAX_VALUE;
        private BufferedStreamWriter writer;

        public FileContentProvider(int frameSize, OutputStream os) throws Exception {
            this.maxTupleSize = (int) (0.80 * frameSize) / 2;
            this.writer = new BufferedStreamWriter(os);
        }

        public FileContentProvider(String[] dir_split, int frameSize) throws Exception {
            this.directorySplits = dir_split;
            this.maxTupleSize = (int) (0.80 * frameSize) / 2;
        }

        private void readChunk(File inputfile, String dirname) throws TException, InterruptedException, IOException {
            FileInputStream fis = new FileInputStream(inputfile);
            BufferedInputStream bis = new BufferedInputStream(new XZInputStream(fis), (32 * 1024));
            final TTransport transport = new TIOStreamTransport(bis);
            final TBinaryProtocol protocol = new TBinaryProtocol(transport);
            transport.open();
            try {
                while (true) {
                    final StreamItem item = new StreamItem();
                    item.read(protocol);

                    KBAStreamDocumentWriter kbadocWriter = new KBAStreamDocumentWriter(item, dirname, writer,
                            maxTupleSize);
                    kbadocWriter.writeToOutput();

                }

            } catch (TTransportException te) {
                // Deal with the EOF exception bug
                if (te.getType() == TTransportException.END_OF_FILE || te.getCause() instanceof java.io.EOFException) {
                    ; // Do nothing:-)
                } else {
                    throw te;
                }
            } finally {
                transport.close();
                fis.close();
            }
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
                        return null;
                        //return readChunk(file, directory.getName());

                    } catch (Exception ex) {
                        throw new RuntimeException("Error getting the stream content", ex);
                    }
                }
            };
            return new SequenceInputStream(inputstream_enumeration);

        }

    }

    private static class KBAStreamServer {
        private final DataProvider dataProvider;
        private final ExecutorService executorService;

        public KBAStreamServer(LinkedBlockingQueue<File> dateHourDirectoryList, int partition, ARecordType outputtype,
                int maxFrameSize, OutputStream os, ExecutorService executorService) throws Exception {
            this.dataProvider = new DataProvider(dateHourDirectoryList, outputtype, partition, maxFrameSize, os);
            this.executorService = executorService;
        }

        public void stop() throws IOException {
            dataProvider.stop();
        }

        public void start() {
            executorService.execute(dataProvider);
        }

    }

    private static class DataProvider implements Runnable {
        private FileContentProvider contentProvider;
        private boolean continuePush = true;
        private LinkedBlockingQueue<File> dateHourDirectoryList;
        private final OutputStream os;
        int numFiles = 0;

        public DataProvider(LinkedBlockingQueue<File> dateHourDirectoryList, ARecordType outputtype, int partition,
                int maxFrameSize, OutputStream os) throws Exception {
            //String corpusDirectory = configuration.get(AsterixTupleParserFactory.KEY_PATH).split(",")[partition];
            this.contentProvider = new FileContentProvider(maxFrameSize, os);
            this.os = os;
            //this.dateHourDirs = KBACorpusFiles.getDateHourDirs(corpusDirectory);
            this.dateHourDirectoryList = dateHourDirectoryList;

        }

        @Override
        public void run() {
            boolean moreData = true;
            while (true) {
                try {
                    while (moreData && continuePush) {
                        File dateHourDir = dateHourDirectoryList.take();
                        moreData = !dateHourDirectoryList.isEmpty();

                        LOGGER.log(Level.INFO, "Reading chunks from " + dateHourDir);

                        final File[] chunks = KBACorpusFiles.getXZFiles(dateHourDir);
                        String dirName = dateHourDir.getName();
                        // Get and feed next batch
                        for (File chunk : chunks) {
                            LOGGER.log(Level.INFO, "File #" + (++this.numFiles) + ". Name: " + chunk.getName());
                            if (this.numFiles == 880)
                                System.out.println("Check!!!");;

                            if (!continuePush)
                                break;
                            contentProvider.readChunk(chunk, dirName);
                        }
                    }
                    os.close();
                    break;
                } catch (Exception e) {
                    e.printStackTrace();
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning("Exception in adaptor " + e.getMessage());
                    }
                }
            }
        }

        public void stop() {
            continuePush = false;
        }

    }

    @Override
    public void stop() throws Exception {
        LOGGER.info(KBAStreamGeneratorAdapter.class.getName() + " stopped successfully.");
        streamDocServer.stop();
    }

    @Override
    public DataExchangeMode getDataExchangeMode() {
        return DataExchangeMode.PUSH;
    }

    @Override
    public boolean handleException(Exception e) {
        try {
            streamDocServer.stop();
        } catch (Exception re) {
            re.printStackTrace();
            return false;
        }
        streamDocServer.start();
        return true;
    }
}