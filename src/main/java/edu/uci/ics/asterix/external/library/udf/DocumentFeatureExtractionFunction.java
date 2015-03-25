package edu.uci.ics.asterix.external.library.udf;

import edu.uci.ics.asterix.external.library.IFunctionHelper;
import edu.uci.ics.asterix.external.library.java.JObjects.JRecord;
import edu.uci.ics.asterix.external.library.udf.featuregeneration.DocumentFeature;

public class DocumentFeatureExtractionFunction extends AbstractFeatureExtractionFunction {
    
    @Override
    public void initialize(IFunctionHelper functionHelper) throws Exception {
        // Get the field positions
        initializeFieldPositions((JRecord) functionHelper.getArgument(0));
        this.docFeature = new DocumentFeature();       
        
    }
}
