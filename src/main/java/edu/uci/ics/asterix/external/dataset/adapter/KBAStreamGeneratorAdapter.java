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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.feeds.api.IFeedAdapter;
import edu.uci.ics.asterix.external.dataset.adapter.StreamBasedAdapter;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

/**
 * Adapter class for creating @see{KBAStreamGeneratorAdapter}.
 * The adapter reads streams of KBA streams from Thrift, and  "push" them into Asterix. 
 * 
 * @author heri
 * 
 */

public class KBAStreamGeneratorAdapter extends StreamBasedAdapter implements IFeedAdapter {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = Logger.getLogger(KBAStreamGeneratorAdapter.class.getName());

    private ExecutorService executorService = Executors.newCachedThreadPool();

    private PipedOutputStream outputStream = new PipedOutputStream();

    private PipedInputStream inputStream = new PipedInputStream(outputStream);

    private final KBAStreamServer streamDocServer;

    public KBAStreamGeneratorAdapter(Map<String, String> configuration, ITupleParserFactory parserFactory,
            ARecordType outputtype, int partition, IHyracksTaskContext ctx) throws Exception {
        super(parserFactory, outputtype, ctx, partition);
        this.streamDocServer = new KBAStreamServer(configuration, partition, outputtype, outputStream, executorService);
    }

    @Override
    public void start(int partition, IFrameWriter writer) throws Exception {
        streamDocServer.start();
        super.start(partition, writer);
    }

    @Override
    public InputStream getInputStream(int partition) throws IOException {
        return inputStream;
    }

    private static class KBAStreamServer {
        private final DataProvider dataProvider;
        private final ExecutorService executorService;

        public KBAStreamServer(Map<String, String> configuration, int partition,  ARecordType outputtype, OutputStream os,
                ExecutorService executorService) throws Exception {
            dataProvider = new DataProvider(configuration, outputtype, partition, os);
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

    }

    @Override
    public void stop() throws Exception {
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