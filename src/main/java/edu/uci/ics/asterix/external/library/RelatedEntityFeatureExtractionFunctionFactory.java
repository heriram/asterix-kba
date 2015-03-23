package edu.uci.ics.asterix.external.library;

public class RelatedEntityFeatureExtractionFunctionFactory implements IFunctionFactory {

    @Override
    public IExternalFunction getExternalFunction() {
        
        return new RelatedEntityFeatureExtractionFunction();
    }

}
