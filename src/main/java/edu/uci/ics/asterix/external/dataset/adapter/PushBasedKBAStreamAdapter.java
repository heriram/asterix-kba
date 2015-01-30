package edu.uci.ics.asterix.external.dataset.adapter;

import java.util.Map;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.parse.ITupleForwardPolicy;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.runtime.operators.file.AsterixTupleParserFactory;
import edu.uci.ics.asterix.runtime.operators.file.CounterTimerTupleForwardPolicy;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

public class PushBasedKBAStreamAdapter extends ClientBasedFeedAdapter {

    private static final long serialVersionUID = 1L;

    private static final int DEFAULT_BATCH_SIZE = 50000;

    private PushBasedKBAStreamFeedClient kbaStreamClient;

    public PushBasedKBAStreamAdapter(Map<String, String> configuration, ARecordType recordType, IHyracksTaskContext ctx) throws AsterixException {
        super(configuration, ctx);
        this.configuration = configuration;
        this.kbaStreamClient = new PushBasedKBAStreamFeedClient(ctx, recordType, this);
    }

    @Override
    public DataExchangeMode getDataExchangeMode() {
        return DataExchangeMode.PUSH;
    }

    @Override
    public boolean handleException(Exception e) {
        return true;
    }

    @Override
    public IFeedClient getFeedClient(int partition) throws Exception {
        return kbaStreamClient;
    }

    @Override
    public ITupleForwardPolicy getTupleParserPolicy() {
        configuration.put(ITupleForwardPolicy.PARSER_POLICY,
                ITupleForwardPolicy.TupleForwardPolicyType.COUNTER_TIMER_EXPIRED.name());
        String propValue = configuration.get(CounterTimerTupleForwardPolicy.BATCH_SIZE);
        if (propValue == null) {
            configuration.put(CounterTimerTupleForwardPolicy.BATCH_SIZE, "" + DEFAULT_BATCH_SIZE);
        }
        return AsterixTupleParserFactory.getTupleParserPolicy(configuration);
    }

}
