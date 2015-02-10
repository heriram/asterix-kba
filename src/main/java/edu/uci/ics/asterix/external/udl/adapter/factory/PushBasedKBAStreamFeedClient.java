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
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.trec.kba.streamcorpus.StreamItem;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ARecordSerializerDeserializer;
import edu.uci.ics.asterix.external.dataset.adapter.FeedClient;
import edu.uci.ics.asterix.external.dataset.adapter.IFeedClient.InflowState;
import edu.uci.ics.asterix.external.library.utils.AbstractKBAStreamDataprovider;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.runtime.operators.file.CounterTimerTupleForwardPolicy;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

/**
 * An implementation of @see {PushBasedFeedClient} for the KBA Stream service.
 */
public class PushBasedKBAStreamFeedClient extends FeedClient {

    private int batchSize=0;

    private ARecordType recordType;
    private KBAStreamItemProcessor streamItemProcessor;
    private LinkedBlockingQueue<StreamItem> inputQueue;
    private final KBAStreamServer streamDocServer;
    private ExecutorService executorService = Executors.newCachedThreadPool();

    public PushBasedKBAStreamFeedClient(IHyracksTaskContext ctx, ARecordType recordType, int partition, PushBasedKBAStreamAdapter adapter)
            throws AsterixException {
        this.recordType = recordType;
        this.streamItemProcessor = new KBAStreamItemProcessor(recordType);
        this.recordSerDe = new ARecordSerializerDeserializer(recordType);
        this.mutableRecord = streamItemProcessor.getMutableRecord();
        this.initialize(adapter.getConfiguration());
        this.inputQueue = batchSize>0?new LinkedBlockingQueue<StreamItem>(batchSize)
                : new LinkedBlockingQueue<StreamItem>();
        streamDocServer = new KBAStreamServer(adapter.getConfiguration(), partition, recordType, inputQueue, executorService);
        
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
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
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
    
    private static class KBAStreamServer {
        private final DataProvider dataProvider;
        private final ExecutorService executorService;
        
        public KBAStreamServer(Map<String, String> configuration, int partition,  ARecordType outputtype, LinkedBlockingQueue<StreamItem> inputQueue,
                ExecutorService executorService)  {
            
            //TODO Need to check the split list...
            String directories[]=configuration.get("path").split(",");
            dataProvider = new DataProvider(inputQueue, directories[partition]);
            this.executorService = executorService;
        }

        public void stop() throws IOException {
            dataProvider.stop();
        }

        public void start() {
            executorService.execute(dataProvider);
        }

    }
    
    private static class DataProvider extends AbstractKBAStreamDataprovider<StreamItem> implements Runnable {

        public DataProvider(LinkedBlockingQueue<StreamItem> inputQueue, String localDir) {
            super(inputQueue, localDir);
            setElementType(AbstractKBAStreamDataprovider.STREAM_ITEM);
            continueReading = true;
        }

        @Override
        public void stop() {
            continueReading = false;            
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