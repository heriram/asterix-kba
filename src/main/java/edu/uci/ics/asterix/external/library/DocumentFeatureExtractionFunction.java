package edu.uci.ics.asterix.external.library;

import edu.uci.ics.asterix.external.library.java.JObjects.JRecord;
import edu.uci.ics.asterix.external.library.udf.featuregeneration.DocumentFeature;

public class DocumentFeatureExtractionFunction extends AbstractFeatureExtractionFunction {
    
    @Override
    public void initialize(IFunctionHelper functionHelper) throws Exception {
        this.docFeature = new DocumentFeature();       
        // Get the field positions
        initializeFieldPositions((JRecord) functionHelper.getArgument(0));
    }
}
