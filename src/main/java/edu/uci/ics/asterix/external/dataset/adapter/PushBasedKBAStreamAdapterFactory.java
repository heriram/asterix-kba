package edu.uci.ics.asterix.external.dataset.adapter;

import java.io.File;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.feeds.api.IDatasourceAdapter;
import edu.uci.ics.asterix.common.feeds.api.IIntakeProgressTracker;
import edu.uci.ics.asterix.external.util.DNSResolverFactory;
import edu.uci.ics.asterix.external.util.INodeResolver;
import edu.uci.ics.asterix.external.util.INodeResolverFactory;
import edu.uci.ics.asterix.metadata.feeds.IFeedAdapterFactory;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.runtime.operators.file.AsterixTupleParserFactory;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;

public class PushBasedKBAStreamAdapterFactory implements IFeedAdapterFactory {

    private static final long serialVersionUID = 1L;
    
    private static final Logger LOGGER = Logger.getLogger(PushBasedKBAStreamAdapterFactory.class.getName());

    private static final String NAME = "push_kba_stream_feed";
    
    private String KEY_PATH=AsterixTupleParserFactory.KEY_PATH;

    private ARecordType outputType;

    private Map<String, String> configuration;
    
    private FileSplit[] directorySplits;
    
    private static final INodeResolver DEFAULT_NODE_RESOLVER = new DNSResolverFactory().createNodeResolver();
    private INodeResolver nodeResolver;
    

    @Override
    public SupportedOperation getSupportedOperations() {
        return SupportedOperation.READ;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public IDatasourceAdapter createAdapter(IHyracksTaskContext ctx, int partition) throws Exception {
        PushBasedKBAStreamAdapter kbaStreamAdapter = new PushBasedKBAStreamAdapter(configuration, directorySplits, partition, outputType, ctx);
        return kbaStreamAdapter;
    }

    @Override
    public void configure(Map<String, String> configuration, ARecordType outputType) throws Exception {
        this.outputType = outputType;
        this.configuration = configuration;
        String[] splits = ((String) configuration.get(KEY_PATH)).split(",");
        configureFileSplits(splits);
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
    
    @Override
    public AlgebricksPartitionConstraint getPartitionConstraint() throws Exception {
        return configurePartitionConstraint();
    }

    private void configureFileSplits(String[] splits) throws AsterixException {
        if (directorySplits == null) {
            directorySplits = new FileSplit[splits.length];
            String nodeName;
            String nodeLocalDirectoryPath;
            int count = 0;
            String trimmedValue;
            for (String splitPath : splits) {
                trimmedValue = splitPath.trim();
                if (!trimmedValue.contains("://")) {
                    throw new AsterixException("Invalid path: " + splitPath
                            + "\nUsage- path=\"Host://Absolute Directory Path\"");
                }
                nodeName = trimmedValue.split(":")[0];
                nodeLocalDirectoryPath = trimmedValue.split("://")[1];
                FileSplit dirSplit = new FileSplit(nodeName, new FileReference(new File(nodeLocalDirectoryPath)));
                directorySplits[count++] = dirSplit;
            }
        }
    }

    private AlgebricksPartitionConstraint configurePartitionConstraint() throws AsterixException {
        String[] locs = new String[directorySplits.length];
        String location;
        for (int i = 0; i < directorySplits.length; i++) {
            location = getNodeResolver().resolveNode(directorySplits[i].getNodeName());
            locs[i] = location;
        }
        return new AlgebricksAbsolutePartitionConstraint(locs);
    }

    protected INodeResolver getNodeResolver() {
        if (nodeResolver == null) {
            nodeResolver = initializeNodeResolver();
        }
        return nodeResolver;
    }

    private static INodeResolver initializeNodeResolver() {
        INodeResolver nodeResolver = null;
        String configuredNodeResolverFactory = System.getProperty(AsterixTupleParserFactory.NODE_RESOLVER_FACTORY_PROPERTY);
        if (configuredNodeResolverFactory != null) {
            try {
                nodeResolver = ((INodeResolverFactory) (Class.forName(configuredNodeResolverFactory).newInstance()))
                        .createNodeResolver();

            } catch (Exception e) {
                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.log(Level.WARNING, "Unable to create node resolver from the configured classname "
                            + configuredNodeResolverFactory + "\n" + e.getMessage());
                }
                nodeResolver = DEFAULT_NODE_RESOLVER;
            }
        } else {
            nodeResolver = DEFAULT_NODE_RESOLVER;
        }
        return nodeResolver;
    }

}
