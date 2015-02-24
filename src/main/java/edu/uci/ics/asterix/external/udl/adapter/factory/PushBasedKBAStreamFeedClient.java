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
package edu.uci.ics.asterix.external.udl.adapter.factory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.trec.kba.streamcorpus.ContentItem;
import org.trec.kba.streamcorpus.StreamItem;
import org.tukaani.xz.XZInputStream;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ARecordSerializerDeserializer;
import edu.uci.ics.asterix.external.dataset.adapter.FeedClient;
import edu.uci.ics.asterix.external.library.KBATopicEntityLoader;
import edu.uci.ics.asterix.external.library.utils.KBACorpusFiles;
import edu.uci.ics.asterix.external.library.utils.TextAnalysis;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.runtime.operators.file.AsterixTupleParserFactory;
import edu.uci.ics.asterix.runtime.operators.file.CounterTimerTupleForwardPolicy;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

/**
 * An implementation of @see {PushBasedFeedClient} for the KBA Stream service.
 */
public class PushBasedKBAStreamFeedClient extends FeedClient {

    private int batchSize = 0;

    private static final int DEFAULT_BATCH_SIZE = 500;

    private ARecordType recordType;
    private KBAStreamItemProcessor streamItemProcessor;
    private BlockingQueue<Map<String, Object>> dataInputQueue;
    private Map<String, String> configuration;
    private final KBAStreamServer streamDocServer;
    private ExecutorService executorService = Executors.newCachedThreadPool();
    private static String fieldNames[];
    private static final String KEY_FILTER = "pre-filter-mentions";

    public PushBasedKBAStreamFeedClient(IHyracksTaskContext ctx, ARecordType recordType,
            PushBasedKBAStreamAdapter adapter) throws AsterixException {
        this.recordType = recordType;
        this.streamItemProcessor = new KBAStreamItemProcessor(recordType, ctx);
        this.recordSerDe = new ARecordSerializerDeserializer(recordType);
        this.mutableRecord = streamItemProcessor.getMutableRecord();
        this.initialize(adapter.getConfiguration());
        this.dataInputQueue = this.batchSize > 0 ? new ArrayBlockingQueue<Map<String, Object>>(batchSize)
                : new ArrayBlockingQueue<Map<String, Object>>(DEFAULT_BATCH_SIZE);

        String corpusDirectoryName = adapter.getDirectoryFromSplit();

        this.configuration = adapter.getConfiguration();
        streamDocServer = new KBAStreamServer(configuration, corpusDirectoryName, recordType, ctx.getFrameSize(),
                dataInputQueue, executorService);

    }

    public ARecordType getRecordType() {
        return recordType;
    }

    public void startServing() {
        this.streamDocServer.start();
    }

    public void stopServing() {
        try {
            this.streamDocServer.stop();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static class KBAStreamServer {
        private final DataProvider dataProvider;
        private final ExecutorService executorService;
        private File[] dateHourDirectoryList;

        public KBAStreamServer(Map<String, String> configuration, String corpusDirectory, ARecordType outputtype,
                int frameSize, BlockingQueue<Map<String, Object>> dataInputQueue, ExecutorService executorService) {

            dateHourDirectoryList = KBACorpusFiles.getDateHourDirs(corpusDirectory);

            boolean doPrefiltering = false;
            if (configuration.containsKey(KEY_FILTER))
                doPrefiltering = configuration.get(KEY_FILTER).equalsIgnoreCase("yes")
                        || configuration.get(KEY_FILTER).equalsIgnoreCase("y");

            dataProvider = new DataProvider(dateHourDirectoryList, doPrefiltering, outputtype, frameSize,
                    dataInputQueue);
            this.executorService = executorService;
        }

        public void stop() throws IOException {
            dataProvider.stop();
        }

        public void start() {
            executorService.execute(dataProvider);
        }

    }

    private static class FileContentProvider {
        private int maxTupleSize = Integer.MAX_VALUE;
        private BlockingQueue<Map<String, Object>> dataInputQueue;
        private String[][] nameVariants = null;
        private ARecordType recordType;
        private TTransport transport;
        private TBinaryProtocol protocol;

        private boolean doPrefiltering = false;

        public FileContentProvider(int frameSize, BlockingQueue<Map<String, Object>> dataInputQueue) {
            this.maxTupleSize = (int) (0.80 * frameSize) / 2;
            this.dataInputQueue = dataInputQueue;

            nameVariants = KBATopicEntityLoader.loadNameVariants(KBARecord.ANALYZER);
        }

        public FileContentProvider(ARecordType recordType, int frameSize, boolean doFiltering,
                BlockingQueue<Map<String, Object>> dataInputQueue) {
            this.maxTupleSize = (int) (0.80 * frameSize) / 2;
            this.dataInputQueue = dataInputQueue;
            doPrefiltering = doFiltering;

            nameVariants = KBATopicEntityLoader.loadNameVariants(KBARecord.ANALYZER);
            this.recordType = recordType;
        }

        private void readChunk(File inputfile, String dirName) throws Exception {
            FileInputStream fis = new FileInputStream(inputfile);
            BufferedInputStream bis = new BufferedInputStream(new XZInputStream(fis), (32 * 1024));
            this.transport = new TIOStreamTransport(bis);
            this.protocol = new TBinaryProtocol(transport);
            transport.open();
            try {
                while (true) {
                    final StreamItem item = new StreamItem();
                    item.read(protocol);
                    final KBARecord kbaDoc = new KBARecord();

                    kbaDoc.setFieldValues(recordType, item, dirName);
                    fieldNames = kbaDoc.getFieldNames();

                    if (doPrefiltering) {
                        if (kbaDoc.containMention(nameVariants, true))
                            LOGGER.log(Level.INFO,
                                    "Found mentions in " + kbaDoc.getDoc_id() + " - {{" + kbaDoc.getMentionedEntity()
                                            + "}}.");
                    }
                    dataInputQueue.add(kbaDoc.getFields());

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
                bis.close();
            }
        }

    }

    private static class DataProvider implements Runnable {
        private FileContentProvider contentProvider;
        private boolean continuePush = true;
        private File[] dateHourDirectoryList;
        int numFiles = 0;
        int numHours = 0;

        public DataProvider(File[] dateHourDirectoryList, boolean doFiltering, ARecordType outputtype,
                int maxFrameSize, BlockingQueue<Map<String, Object>> dataInputQueue) {
            this.contentProvider = new FileContentProvider(outputtype, maxFrameSize, doFiltering, dataInputQueue);
            this.dateHourDirectoryList = dateHourDirectoryList;
            this.numHours = this.dateHourDirectoryList.length;
        }

        @Override
        public void run() {
            LOGGER.log(Level.INFO, "Feed adapter created and loaded successfully. Now start ingesting data.");
            while (true) {
                try {
                    int index = 0;
                    while ((index < numHours) && continuePush) {
                        File dateHourDir = dateHourDirectoryList[index];

                        LOGGER.log(Level.INFO, "Reading chunks from " + dateHourDir);

                        final File[] chunks = KBACorpusFiles.getXZFiles(dateHourDir);
                        String dirName = dateHourDir.getName();
                        // Get and feed next batch
                        for (File chunk : chunks) {
                            if (((++this.numFiles) % 50) == 0) {
                                LOGGER.log(Level.INFO, "Processed " + (this.numFiles) + " files out of "
                                        + (chunks.length * this.numHours) + ".");
                            }
                            if (!continuePush)
                                break;
                            contentProvider.readChunk(chunk, dirName);
                        }
                        index++;
                    }
                    break;
                } catch (Exception e) {
                    e.printStackTrace();
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning("Exception in adaptor " + e.getMessage());
                    }
                }
            }
            LOGGER.log(Level.INFO, "Reached the end of this feed - i.e., no more data to push.");

        }

        public void stop() {
            continuePush = false;
        }

    }

    @Override
    public InflowState retrieveNextRecord() throws Exception {
        streamItemProcessor.processNextStreamDocument(dataInputQueue.take(), fieldNames);
        return InflowState.DATA_AVAILABLE;
    }

    private void initialize(Map<String, String> params) {
        if (params.containsKey(CounterTimerTupleForwardPolicy.BATCH_SIZE))
            this.batchSize = Integer.parseInt(params.get(CounterTimerTupleForwardPolicy.BATCH_SIZE));
    }

}