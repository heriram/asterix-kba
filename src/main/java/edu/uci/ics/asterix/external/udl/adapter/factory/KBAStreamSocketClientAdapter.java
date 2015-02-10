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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.feeds.api.IFeedAdapter;
import edu.uci.ics.asterix.external.dataset.adapter.StreamBasedAdapter;
import edu.uci.ics.asterix.external.library.utils.AbstractKBAStreamDataprovider;
import edu.uci.ics.asterix.external.library.utils.BufferedStreamWriter;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.tools.external.data.GenericSocketFeedAdapter;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

public class KBAStreamSocketClientAdapter extends StreamBasedAdapter implements IFeedAdapter {



    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = Logger.getLogger(KBAStreamSocketClientAdapter.class.getName());

    private static final String LOCALHOST = "127.0.0.1";

    private static final long RECONNECT_PERIOD = 2000;

    private final String localDir;

    private final int port;

    private final IHyracksTaskContext ctx;

    private GenericSocketFeedAdapter socketFeedAdapter;
    
    private static boolean continueStreaming = true;
    private LinkedBlockingQueue<String> inputQueue;
    private int batchSize = 0;

    private KBAStreamServer kbaStreamServer;

    public KBAStreamSocketClientAdapter(ITupleParserFactory parserFactory, ARecordType outputType,
            IHyracksTaskContext ctx, int partition, int port, String localDir) throws Exception {
        super(parserFactory, outputType, ctx, partition);
        socketFeedAdapter = new GenericSocketFeedAdapter(parserFactory, outputType, port, ctx, partition);
        this.inputQueue = batchSize > 0 ? new LinkedBlockingQueue<String>(batchSize)
                : new LinkedBlockingQueue<String>();
        this.localDir = localDir;
        this.port = port;
        this.ctx = ctx;
    }
    
    @Override
    public InputStream getInputStream(int partition) throws IOException {
       return socketFeedAdapter.getInputStream(partition);
    }
    
    @Override
    public void start(int partition, IFrameWriter writer) throws Exception {
        socketFeedAdapter.start(partition, writer);
        Socket socket = waitForReceiver();
        OutputStream os = socket.getOutputStream();
        ExecutorService executorService = Executors.newCachedThreadPool();
        kbaStreamServer = new KBAStreamServer(this.inputQueue, this.localDir, executorService);
        kbaStreamServer.start();
        BufferedStreamWriter bufferedWriter = new BufferedStreamWriter(os);

        try {
            while (continueStreaming) {
                String streamString = this.inputQueue.take();
                bufferedWriter.writeStreamADMString(streamString);
            }
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Finished streaming file " + localDir + "to port [" + port + "]");
            }

        } finally {
            socket.close();
        }

    }
    
    private Socket waitForReceiver() throws Exception {
        Socket socket = null;
        while (socket == null) {
            try {
                socket = new Socket(LOCALHOST, port);
            } catch (Exception e) {
                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.warning("Receiver not ready, would wait for " + (RECONNECT_PERIOD / 1000)
                            + " seconds before reconnecting");
                }
                Thread.sleep(RECONNECT_PERIOD);
            }
        }
        return socket;
    }

    private static class KBAStreamServer {
        private final KBADataProvider dataProvider;
        private final ExecutorService executorService;

        public KBAStreamServer(LinkedBlockingQueue<String> inputQ, String localDir, ExecutorService executorService)
                throws Exception {
            dataProvider = new KBADataProvider(inputQ, localDir);
            this.executorService = executorService;
        }

        public void stop() throws IOException {
            dataProvider.stop();
        }

        public void start() {
            executorService.execute(dataProvider);
        }

    }

    private static class KBADataProvider extends AbstractKBAStreamDataprovider {

        public KBADataProvider(LinkedBlockingQueue<String> inputQ, String localDir) {
            super(inputQ, localDir);
        }

        @Override
        public void stop() {
            continueStreaming = false;
        }

    }



    @Override
    public DataExchangeMode getDataExchangeMode() {
        return DataExchangeMode.PUSH;
    }

    @Override
    public void stop() throws Exception {
        continueStreaming = false;
        socketFeedAdapter.stop();
    }

    @Override
    public boolean handleException(Exception e) {
        try {
            kbaStreamServer.stop();
        } catch (Exception re) {
            re.printStackTrace();
            return false;
        }
        kbaStreamServer.start();
        return true;
    }

}
