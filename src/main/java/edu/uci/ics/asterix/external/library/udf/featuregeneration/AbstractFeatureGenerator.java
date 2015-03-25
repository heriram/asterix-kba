package edu.uci.ics.asterix.external.library.udf.featuregeneration;

import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.asterix.external.library.IFunctionHelper;
import edu.uci.ics.asterix.external.library.JTypeObjectFactory;
import edu.uci.ics.asterix.external.library.TopicEntity;
import edu.uci.ics.asterix.external.library.java.IJObject;
import edu.uci.ics.asterix.external.library.java.JObjects.JDouble;
import edu.uci.ics.asterix.external.library.java.JObjects.JInt;
import edu.uci.ics.asterix.external.library.java.JObjects.JObject;
import edu.uci.ics.asterix.external.library.java.JObjects.JOrderedList;
import edu.uci.ics.asterix.external.library.java.JObjects.JRecord;
import edu.uci.ics.asterix.external.library.java.JObjects.JString;
import edu.uci.ics.asterix.external.library.java.JTypeTag;
import edu.uci.ics.asterix.external.library.textanalysis.Tokenizer;
import edu.uci.ics.asterix.external.udl.adapter.factory.KBARecord;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.container.IObjectPool;
import edu.uci.ics.asterix.om.util.container.ListObjectPool;



/**
 * Abstract class for generating a particular set of features for a given
 * stream_id-urlname pair.
 * 
 * @author Krisztian Balog <krisztian.balog@idi.ntnu.no>
 * @author Heri Ramampiaro <heri@idi.ntnu.no>
 */
public abstract class AbstractFeatureGenerator {
    protected IFunctionHelper functionHelper;

    protected String prefix = "";

    protected Tokenizer tokenizer;

    protected HashMap<String, String> props;

    public AbstractFeatureGenerator() {
        this.tokenizer = Tokenizer.INSTANCE;
    }
    
    protected final IObjectPool<IJObject, IAType> objectPool = new ListObjectPool<IJObject, IAType>(
            JTypeObjectFactory.INSTANCE);
    
    
    public AbstractFeatureGenerator(HashMap<String, String> props) {
        this.props = props;
        this.tokenizer = Tokenizer.INSTANCE;
    }
 
    
    public abstract String[] getFeatureNames();

    public abstract String[] getFeatureTypes();

    protected JObject setValue(JTypeTag typeTag, Object value) {
        JObject newObject = null;        
        switch(typeTag) {
            case INT:
                newObject = (JInt) functionHelper.getObject(JTypeTag.INT);
                ((JInt)newObject).setValue((int)value);
                break;
            case FLOAT:
            case DOUBLE:
                newObject = (JDouble) objectPool.allocate(BuiltinType.ADOUBLE);
                ((JDouble)newObject).setValue((double)value);
                break;
            default:
                newObject = (JString) functionHelper.getObject(JTypeTag.STRING);
                ((JString)newObject).setValue((String)value);
                break;
        }
        return newObject;
    }
    
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
    
    protected String conctatenateBodyContent(JOrderedList bodyContent) {
        StringBuilder sb= new StringBuilder();
        
        JString jString =  (JString) bodyContent.getElement(0);
        sb.append(jString.getValue());
        for (int i=1; i<bodyContent.size(); i++) {
            jString =  (JString) bodyContent.getElement(i);
            sb.append(" " + jString.getValue());
        }
        return sb.toString();
    }

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
