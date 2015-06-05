package edu.uci.ics.asterix.external.dataset.adapter;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.thrift.transport.TTransportException;
import org.trec.kba.streamcorpus.StreamItem;
import org.tukaani.xz.XZInputStream;

import edu.uci.ics.asterix.external.library.udf.featuregeneration.EntitySearcher;
import edu.uci.ics.asterix.external.library.utils.FileTBinaryProtocol;
import edu.uci.ics.asterix.external.library.utils.FileTIOStreamTransport;
import edu.uci.ics.asterix.external.library.utils.LanguageDetector;
import edu.uci.ics.asterix.external.library.utils.TupleUtils;
import edu.uci.ics.asterix.om.types.ARecordType;

public class KBAFileReader {
    protected static final Logger LOGGER = Logger.getLogger(KBAFileReader.class.getName());
    private int maxTupleSize = Integer.MAX_VALUE;
    private BlockingQueue<Map<String, Object>> dataInputQueue;
    private ARecordType recordType;

    private StreamItem streamItem;
    private KBARecord kbaDoc;
    private EntitySearcher mentionSearcher;
    private Map<String, String> entityURLMap;
    private boolean doPrefiltering = false;
    private boolean initialized = false;

    public KBAFileReader(ARecordType recordType, int frameSize, boolean doFiltering,
            BlockingQueue<Map<String, Object>> dataInputQueue) {
        this.maxTupleSize = (int) (0.80 * frameSize) / 2;
        this.dataInputQueue = dataInputQueue;
        doPrefiltering = doFiltering;

        this.streamItem = new StreamItem();

        this.recordType = recordType;
    }

    public KBAFileReader(ARecordType recordType, int frameSize, boolean doFiltering,
            BlockingQueue<Map<String, Object>> dataInputQueue, LanguageDetector languageDetector,
            EntitySearcher mentionSearcher, Map<String, String> entityURLMap) {
        this(recordType, frameSize, doFiltering, dataInputQueue);
        initialize(languageDetector, mentionSearcher, entityURLMap);
    }

    public void initialize(LanguageDetector languageDetector, EntitySearcher mentionSearcher,
            Map<String, String> entityURLMap) {
        this.kbaDoc = new KBARecord(languageDetector);
        this.entityURLMap = entityURLMap;
        this.mentionSearcher = mentionSearcher;
        initialized = true;

    }

    public void readChunk(File inputfile, String dirName) throws Exception {
        readChunk(inputfile, dirName, false);
    }

    public void readChunk(File inputfile, String dirName, boolean verbose) throws Exception {
        if (!initialized) {
            LOGGER.log(Level.SEVERE, "File reader not initialized properly. Please run initialize() first.");
            return;
        }

        BufferedInputStream bis = new BufferedInputStream(new XZInputStream(new FileInputStream(inputfile)),
                (32 * 1024));

        FileTBinaryProtocol protocol = FileTBinaryProtocol.INSTANCE;
        protocol.reset();
        protocol.initializeFileTransport(FileTIOStreamTransport.INSTANCE, bis);
        try {
            streamItem.clear();
            while (true) {
                streamItem.read(protocol);

                kbaDoc.setFieldValues(recordType, streamItem, dirName);

                if (!kbaDoc.isEmpty()) {
                    if (doPrefiltering) {
                        boolean foundMention = kbaDoc.containMention(mentionSearcher, entityURLMap);
                        if (foundMention && verbose)
                            LOGGER.log(Level.INFO,
                                    "Found mentions in " + kbaDoc.getDoc_id() + " - {{" + kbaDoc.getMentionedEntity()
                                            + "}}.");
                    }

                    // Get the field contents
                    Map<String, Object> fields = kbaDoc.getFields();
                    int tupleSize = TupleUtils.getTupleSize(fields);
                    if (tupleSize <= maxTupleSize) {
                        dataInputQueue.add(fields);
                    } else {
                        TupleUtils.splitTuple(fields, tupleSize, maxTupleSize, dataInputQueue);
                    }
                }

            }

        } catch (TTransportException te) {
            // Deal with the EOF exception bug
            if (te.getType() == TTransportException.END_OF_FILE || te.getCause() instanceof java.io.EOFException) {
                ; // Do nothing:-)
            } else {
                throw te;
            }
        }
    }

}
