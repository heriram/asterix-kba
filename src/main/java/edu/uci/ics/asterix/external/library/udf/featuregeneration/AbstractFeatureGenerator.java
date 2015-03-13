package edu.uci.ics.asterix.external.library.udf.featuregeneration;

import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.asterix.external.library.IFunctionHelper;
import edu.uci.ics.asterix.external.library.TopicEntity;
import edu.uci.ics.asterix.external.library.java.JObjects.JRecord;
import edu.uci.ics.asterix.external.library.textanalysis.Tokenizer;
import edu.uci.ics.asterix.external.udl.adapter.factory.KBARecord;



/**
 * Abstract class for generating a particular set of features for a given
 * stream_id-urlname pair.
 * 
 * @author Krisztian Balog <krisztian.balog@idi.ntnu.no>
 * @author Heri Ramampiaro <heri@idi.ntnu.no>
 */
public abstract class AbstractFeatureGenerator {

    protected String prefix = "";

    protected Tokenizer tokenizer;

    protected HashMap<String, String> props;

    public AbstractFeatureGenerator(HashMap<String, String> props) {
        this.props = props;
        this.tokenizer = Tokenizer.INSTANCE;
    }

    public abstract String[] getFeatureNames();

    public abstract String[] getFeatureTypes();

    /**
     * Generate feature vector for a document-entity pair.
     * 
     * @param streamDoc
     *            StreamDocument corresponding to a stream-id
     * @param entity
     *            Target entity corresponding to urlname
     * @return
     */

    public abstract String getFeatureVector(KBARecord streamDoc, TopicEntity entity);
    public abstract JRecord getResultRecord(IFunctionHelper functionHelper, Map<String, Integer> fieldPositions);
    

    /**
     * Generate Arff header with feature names and types (i.e., "@ATTRIBUTE"
     * lines)
     * 
     * @return
     */
    public String getArffHeader() {
        String header = "";
        String[] featureNames = getFeatureNames();
        String[] featureTypes = getFeatureTypes();

        for (int i = 0; i < featureNames.length; i++) {
            header += "@ATTRIBUTE " + prefix + featureNames[i] + " " + featureTypes[i] + "\n";
        }
        return header;
    }

    public void setPrefix(String featurePrefix) {
        this.prefix = featurePrefix;
    }

}
