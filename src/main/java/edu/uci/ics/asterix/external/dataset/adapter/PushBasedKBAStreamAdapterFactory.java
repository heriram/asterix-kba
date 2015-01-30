package edu.uci.ics.asterix.external.dataset.adapter;

import java.util.Map;

import edu.uci.ics.asterix.common.feeds.api.IDatasourceAdapter;
import edu.uci.ics.asterix.common.feeds.api.IIntakeProgressTracker;
import edu.uci.ics.asterix.external.dataset.adapter.PushBasedTwitterAdapter;
import edu.uci.ics.asterix.external.util.TwitterUtil;
import edu.uci.ics.asterix.metadata.feeds.IFeedAdapterFactory;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksCountPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

public class PushBasedKBAStreamAdapterFactory implements IFeedAdapterFactory {

    private static final long serialVersionUID = 1L;

    private static final String NAME = "push_kba_stream_feed";

    private ARecordType outputType;

    private Map<String, String> configuration;

    @Override
    public SupportedOperation getSupportedOperations() {
        return SupportedOperation.READ;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public AlgebricksPartitionConstraint getPartitionConstraint() throws Exception {
        return new AlgebricksCountPartitionConstraint(1);
    }

    @Override
    public IDatasourceAdapter createAdapter(IHyracksTaskContext ctx, int partition) throws Exception {
        PushBasedKBAStreamAdapter kbaStreamAdapter = new PushBasedKBAStreamAdapter(configuration, outputType, ctx);
        return kbaStreamAdapter;
    }

    @Override
    public void configure(Map<String, String> configuration, ARecordType outputType) throws Exception {
        this.outputType = outputType;
        this.configuration = configuration;
    }

    @Override
    public ARecordType getAdapterOutputType() {
        return outputType;
    }
    

    @Override
    public boolean isRecordTrackingEnabled() {
        return false;
    }

    @Override
    public IIntakeProgressTracker createIntakeProgressTracker() {
        return null;
    }

}
