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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ARecordSerializerDeserializer;
import edu.uci.ics.asterix.external.library.KBATopicEntityLoader;
import edu.uci.ics.asterix.external.library.udf.featuregeneration.EntitySearcher;
import edu.uci.ics.asterix.external.library.utils.KBACorpusFiles;
import edu.uci.ics.asterix.external.library.utils.LanguageDetector;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.runtime.operators.file.CounterTimerTupleForwardPolicy;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

/**
 * An implementation of @see {PushBasedFeedClient} for the KBA Stream service.
 */
public class KBAPushBasedStreamFeedClient extends FeedClient {

    private int batchSize = 0;

    private static final int DEFAULT_BATCH_SIZE = 500;

    private ARecordType recordType;
    private KBAStreamItemProcessor streamItemProcessor;
    private BlockingQueue<Map<String, Object>> dataInputQueue;
    private final KBAStreamServer streamDocServer;
    private ExecutorService executorService = Executors.newCachedThreadPool();
    private static String fieldNames[];
    private static final String KEY_FILTER = "pre-filter-mentions";

    private static Map<String, String> entityURLMap;

    private static EntitySearcher mentionSearcher;
    private static final LanguageDetector languageDetector = new LanguageDetector();

    public KBAPushBasedStreamFeedClient(IHyracksTaskContext ctx, ARecordType recordType,
            PushBasedKBAStreamAdapter adapter) throws AsterixException {

        Map<String, String> configuration = adapter.getConfiguration();

        Set<String> nameVariants = new HashSet<String>();
        KBATopicEntityLoader.loadNameVariants(nameVariants);
        mentionSearcher = new EntitySearcher(nameVariants);
        nameVariants.clear();

        entityURLMap = new HashMap<String, String>();
        KBATopicEntityLoader.buildNameURLMap(entityURLMap);

        this.recordType = recordType;
        this.streamItemProcessor = new KBAStreamItemProcessor(recordType, ctx);
        this.recordSerDe = new ARecordSerializerDeserializer(recordType);
        this.mutableRecord = streamItemProcessor.getMutableRecord();
        this.initialize(configuration);
        this.dataInputQueue = this.batchSize > 0 ? new LinkedBlockingQueue<Map<String, Object>>(batchSize)
                : new ArrayBlockingQueue<Map<String, Object>>(DEFAULT_BATCH_SIZE);

        String corpusDirectoryName = adapter.getDirectoryFromSplit();

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

    public void cleanUp() {
        entityURLMap.clear();
        dataInputQueue.clear();
        streamItemProcessor.reset();
    }

    private static class KBAStreamServer {
        private final KBADataProvider dataProvider;
        private final ExecutorService executorService;
        private File[] dateHourDirectoryList;

        public KBAStreamServer(Map<String, String> configuration, String corpusDirectory, ARecordType outputtype,
                int frameSize, BlockingQueue<Map<String, Object>> dataInputQueue, ExecutorService executorService) {

            dateHourDirectoryList = KBACorpusFiles.getDateHourDirs(corpusDirectory);

            boolean doPrefiltering = false;
            if (configuration.containsKey(KEY_FILTER))
                doPrefiltering = configuration.get(KEY_FILTER).equalsIgnoreCase("yes")
                        || configuration.get(KEY_FILTER).equalsIgnoreCase("y");

            KBAFileReader chunkReader = new KBAFileReader(outputtype, frameSize, doPrefiltering, dataInputQueue,
                    languageDetector, mentionSearcher, entityURLMap);
            
            fieldNames = outputtype.getFieldNames();

            dataProvider = new KBADataProvider(dateHourDirectoryList, outputtype, frameSize, dataInputQueue, chunkReader);

            this.executorService = executorService;
        }

        public void stop() throws IOException {
            dataProvider.stop();
        }

        public void start() {
            executorService.execute(dataProvider);
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