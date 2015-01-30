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
import java.io.OutputStream;
import java.net.Socket;
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
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

public class KBAStreamSocketClientAdapter implements IFeedAdapter {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = Logger.getLogger(KBAStreamSocketClientAdapter.class.getName());

    private static final String LOCALHOST = "127.0.0.1";

    private static final long RECONNECT_PERIOD = 2000;

    private final String localDir;

    private final int port;

    private final IHyracksTaskContext ctx;

    private static boolean continueStreaming = true;
    private LinkedBlockingQueue<String> inputQueue;
    private int batchSize = 0;

    private KBAStreamServer kbaStreamServer;

    public KBAStreamSocketClientAdapter(Integer port, String localDir, IHyracksTaskContext ctx) {
        this.inputQueue = batchSize > 0 ? new LinkedBlockingQueue<String>(batchSize)
                : new LinkedBlockingQueue<String>();
        this.localDir = localDir;
        this.port = port;
        this.ctx = ctx;
    }

    @Override
    public void start(int partition, IFrameWriter writer) throws Exception {
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

    private static class KBADataProvider implements Runnable {

        private static final int BUFFER_LENGTH = 8 * 1024;
        private LinkedBlockingQueue<String> inputQueue;
        private File[] dirDateHourDirs;

        public KBADataProvider(LinkedBlockingQueue<String> inputQ, String localDir) {
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
