package edu.uci.ics.asterix.external.library;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.uci.ics.asterix.external.library.java.JObjects.JRecord;
import edu.uci.ics.asterix.external.library.udf.featuregeneration.AbstractFeatureGenerator;
import edu.uci.ics.asterix.external.library.udf.featuregeneration.EntitySearcher;
import edu.uci.ics.asterix.external.library.utils.TupleUtils;
import edu.uci.ics.asterix.external.udl.adapter.factory.KBARecord;

public abstract class AbstractFeatureExtractionFunction implements IExternalScalarFunction {
    protected EntitySearcher searcher;
    protected Map<String, Integer> fieldPositions;
    protected AbstractFeatureGenerator docFeature;
    
    private Set<String> nameVariants;
    
    protected void initializeSearcher() {
        // Initialize the entity mention finder, including building the inverted list
        this.nameVariants = new HashSet<String>();
        KBATopicEntityLoader.loadNameVariants(nameVariants);
        this.searcher = new EntitySearcher(nameVariants);
    }
    
    protected void initializeFieldPositions(JRecord inputRecord) {
        this.fieldPositions = new HashMap<String, Integer>();
        String fieldNames[] = { KBARecord.FIELD_TITLE, KBARecord.FIELD_BODY, KBARecord.FIELD_ANCHOR,
                KBARecord.FIELD_SOURCE, KBARecord.FIELD_LANGUAGE };

        for (String field : fieldNames) {
            fieldPositions.put(field, TupleUtils.getFieldPosByName(inputRecord, field));
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
