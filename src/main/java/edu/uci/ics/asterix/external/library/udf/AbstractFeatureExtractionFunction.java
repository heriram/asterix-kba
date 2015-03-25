package edu.uci.ics.asterix.external.library.udf;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.uci.ics.asterix.external.library.IExternalScalarFunction;
import edu.uci.ics.asterix.external.library.IFunctionHelper;
import edu.uci.ics.asterix.external.library.KBATopicEntityLoader;
import edu.uci.ics.asterix.external.library.java.JObjects.JRecord;
import edu.uci.ics.asterix.external.library.udf.featuregeneration.AbstractFeatureGenerator;
import edu.uci.ics.asterix.external.library.udf.featuregeneration.EntitySearcher;
import edu.uci.ics.asterix.external.library.utils.TupleUtils;
import edu.uci.ics.asterix.external.udl.adapter.factory.KBARecord;
import edu.uci.ics.asterix.om.types.ARecordType;

public abstract class AbstractFeatureExtractionFunction implements IExternalScalarFunction {
    protected EntitySearcher searcher;
    protected Map<String, Integer> fieldPositions;
    protected AbstractFeatureGenerator docFeature;
    
    private Set<String> nameVariants;
    
    /*
     *  Initialize the entity mention finder, including building the inverted list
     */
    protected void initializeSearcher() {
       
        this.nameVariants = new HashSet<String>();
        KBATopicEntityLoader.loadNameVariants(nameVariants);
        this.searcher = new EntitySearcher(nameVariants);
    }
    
    /*
     * Initialize the field positions for fast lookup in the feed functions
     */
    protected void initializeFieldPositions(JRecord inputRecord) {
        this.fieldPositions = new HashMap<String, Integer>();
        
        ARecordType recordType = inputRecord.getRecordType();
        String fieldNames[] = recordType.getFieldNames();
        
        int index = 0;
        for (String field : fieldNames) {
            fieldPositions.put(field, index);
            index++;
        }
    }
    
    @Override
    public void evaluate(IFunctionHelper functionHelper) throws Exception {
        functionHelper.setResult(docFeature.getResultRecord(functionHelper, fieldPositions));
    }

    @Override
    public void deinitialize() {
        nameVariants = null;
        searcher = null;
        fieldPositions = null;
    }
}
