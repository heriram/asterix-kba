package edu.uci.ics.asterix.external.library.udf;

import edu.uci.ics.asterix.external.library.IFunctionHelper;
import edu.uci.ics.asterix.external.library.java.JObjects.JRecord;
import edu.uci.ics.asterix.external.library.udf.featuregeneration.DocumentEntityFeature;
import edu.uci.ics.asterix.external.library.udf.featuregeneration.DocumentFeature;

public class DocumentEntityFeatureExtractionFunction extends AbstractFeatureExtractionFunction {
    
    @Override
    public void initialize(IFunctionHelper functionHelper) throws Exception { 
        // Initialize the searcher
        initializeSearcher();
        
        // Get the field positions
        initializeFieldPositions((JRecord) functionHelper.getArgument(0));
        
        this.docFeature = new DocumentEntityFeature(searcher); 
    }
}
