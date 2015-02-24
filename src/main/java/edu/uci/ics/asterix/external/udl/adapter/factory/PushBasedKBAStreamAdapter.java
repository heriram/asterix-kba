package edu.uci.ics.asterix.external.udl.adapter.factory;

import java.io.File;
import java.util.Map;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.parse.ITupleForwardPolicy;
import edu.uci.ics.asterix.external.dataset.adapter.ClientBasedFeedAdapter;
import edu.uci.ics.asterix.external.dataset.adapter.IFeedClient;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.runtime.operators.file.AsterixTupleParserFactory;
import edu.uci.ics.asterix.runtime.operators.file.CounterTimerTupleForwardPolicy;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;

public class PushBasedKBAStreamAdapter extends ClientBasedFeedAdapter {

    private static final long serialVersionUID = 1L;

    private static final int DEFAULT_BATCH_SIZE = 50000;

    FileSplit directorySplit;

    private PushBasedKBAStreamFeedClient kbaStreamClient;

    public PushBasedKBAStreamAdapter(Map<String, String> configuration, FileSplit directorySplit[], int partition,
            ARecordType recordType, IHyracksTaskContext ctx) throws AsterixException {
        super(configuration, ctx);
        this.configuration = configuration;
        this.directorySplit = directorySplit[partition];
        this.kbaStreamClient = new PushBasedKBAStreamFeedClient(ctx, recordType, this);
    }

    @Override
    public DataExchangeMode getDataExchangeMode() {
        return DataExchangeMode.PUSH;
    }

    @Override
    public boolean handleException(Exception e) {
        try {
            kbaStreamClient.stopServing();
        } catch (Exception re) {
            re.printStackTrace();
            return false;
        }
        kbaStreamClient.startServing();
        return true;
    }

    @Override
    public IFeedClient getFeedClient(int partition) throws Exception {
        kbaStreamClient.startServing();
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

    @Override
    public void stop() throws Exception {
        kbaStreamClient.stopServing();
        continueIngestion = false;
    }

    /**
     * Get the directory from the split
     * 
     * @return
     */
    public String getDirectoryFromSplit() {
        File directory = directorySplit.getLocalFile().getFile();
        return directory.getAbsolutePath();
    }

}
