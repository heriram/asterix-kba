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

import java.util.Map;

import edu.uci.ics.asterix.common.feeds.api.IDatasourceAdapter;
import edu.uci.ics.asterix.common.feeds.api.IIntakeProgressTracker;
import edu.uci.ics.asterix.external.adapter.factory.StreamBasedAdapterFactory;
import edu.uci.ics.asterix.metadata.feeds.IFeedAdapterFactory;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.runtime.operators.file.AsterixTupleParserFactory;
import edu.uci.ics.asterix.runtime.operators.file.AsterixTupleParserFactory.InputDataFormat;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksCountPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

/**
 * Factory class for creating @see{KBAStreamGeneratorAdapter}.
 * The adapter reads streams of KBA streams from Thrift, and "push" them into Asterix.
 * 
 * @author heri
 */

public class KBAStreamGeneratorAdapterFactory extends StreamBasedAdapterFactory implements IFeedAdapterFactory {

    private static final long serialVersionUID = 1L;

    private ARecordType outputType = null;

    @Override
    public String getName() {
        return "kba_stream_feeder";
    }

    @Override
    public SupportedOperation getSupportedOperations() {
        return SupportedOperation.READ;
    }

    @Override
    public void configure(Map<String, String> configuration, ARecordType outputType) throws Exception {
        configuration.put(AsterixTupleParserFactory.KEY_FORMAT, AsterixTupleParserFactory.FORMAT_ADM);
        this.configuration = configuration;
        this.outputType = outputType;
        this.configureFormat(outputType);
    }

    @Override
    public AlgebricksPartitionConstraint getPartitionConstraint() throws Exception {
        return new AlgebricksCountPartitionConstraint(1);
    }

    @Override
    public IDatasourceAdapter createAdapter(IHyracksTaskContext ctx, int partition) throws Exception {
        return new KBAStreamGeneratorAdapter(configuration, parserFactory, outputType, partition, ctx);
    }

    @Override
    public ARecordType getAdapterOutputType() {
        return outputType;
    }

    @Override
    public InputDataFormat getInputDataFormat() {
        return InputDataFormat.ADM;
    }

    public boolean isRecordTrackingEnabled() {
        return false;
    }

    public IIntakeProgressTracker createIntakeProgressTracker() {
        throw new UnsupportedOperationException("Tracking of ingested records not enabled");
    }

}