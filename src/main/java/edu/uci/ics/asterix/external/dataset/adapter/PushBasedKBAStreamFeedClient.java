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
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.trec.kba.streamcorpus.StreamItem;
import org.tukaani.xz.XZInputStream;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ARecordSerializerDeserializer;
import edu.uci.ics.asterix.external.library.utils.KBACorpusFiles;
import edu.uci.ics.asterix.external.library.utils.KBAStreamItemProcessor;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.runtime.operators.file.CounterTimerTupleForwardPolicy;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

/**
 * An implementation of @see {PullBasedFeedClient} for the Twitter service. The
 * feed client fetches data from Twitter service by sending request at regular
 * (configurable) interval.
 */
public class PushBasedKBAStreamFeedClient extends FeedClient {

    private int batchSize=0;

    private ARecordType recordType;
    private KBAStreamItemProcessor streamItemProcessor;
    private LinkedBlockingQueue<StreamItem> inputQueue;

    public PushBasedKBAStreamFeedClient(IHyracksTaskContext ctx, ARecordType recordType, PushBasedKBAStreamAdapter adapter)
            throws AsterixException {
        this.recordType = recordType;
        this.streamItemProcessor = new KBAStreamItemProcessor(recordType);
        this.recordSerDe = new ARecordSerializerDeserializer(recordType);
        this.mutableRecord = streamItemProcessor.getMutableRecord();
        this.initialize(adapter.getConfiguration());
        this.inputQueue = batchSize>0?new LinkedBlockingQueue<StreamItem>(batchSize)
                : new LinkedBlockingQueue<StreamItem>();
        StreamItemReader streamReader = new StreamItemReader(inputQueue);
        Thread streamProducer = new Thread(streamReader);
        
        //TODO Check where to best put this
        streamProducer.start();
    }

    public ARecordType getRecordType() {
        return recordType;
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
    
    private class StreamItemReader implements Runnable {
        private static final int BUFFER_LENGTH = 8 * 1024;
        private LinkedBlockingQueue<StreamItem> inputQueue;
        private File[] dirDateHourDirs;

        public StreamItemReader(LinkedBlockingQueue<StreamItem> inputQ) {
            this.inputQueue = inputQ;
            dirDateHourDirs = KBACorpusFiles.getDateHourDirs();
        }

        private InputStream getInputStream(File in_file) {
            try {
                FileInputStream fis = new FileInputStream(in_file);
                BufferedInputStream file = new BufferedInputStream(new XZInputStream(fis), BUFFER_LENGTH);
                return file;

            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }

        private void readStremItem() throws TException, InterruptedException {

            for (File datehourdir : dirDateHourDirs) {

                File xz_files[] = KBACorpusFiles.getXZFiles(datehourdir);

                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Processing files in " + datehourdir.getName());
                }

                // Reading the thrift-based (by default, xz-compressed) stream files
                for (File file:xz_files) {
                    final TTransport transport = new TIOStreamTransport(getInputStream(file));
                    final TBinaryProtocol protocol = new TBinaryProtocol(transport);
                    transport.open();
                    try {
                        while (true) {
                            final StreamItem item = new KBAStreamItem(datehourdir.getName());
                            item.read(protocol);
                            inputQueue.put(item);
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
                }
               }
         }

        @Override
        public void run() {
            try {
                readStremItem();
            } catch (TException e) {
                LOGGER.log(Level.WARNING, "Thrift Exception when trying to read the stream item. " + e.getMessage());
            } catch (InterruptedException e) {
                LOGGER.log(Level.WARNING, "Interruption Exception when trying to put an element in the inputQueue. " + e.getMessage());
            }
        }

    }

    @Override
    public InflowState retrieveNextRecord() throws Exception {
        KBAStreamItem streamItem = (KBAStreamItem) inputQueue.take();
        streamItemProcessor.processNextStreamDocument(streamItem, streamItem.getDirName());
        return InflowState.DATA_AVAILABLE;
    }

    private void initialize(Map<String, String> params) {
        if (params.containsKey(CounterTimerTupleForwardPolicy.BATCH_SIZE))
        this.batchSize = Integer.parseInt(params.get(CounterTimerTupleForwardPolicy.BATCH_SIZE));
    }

}