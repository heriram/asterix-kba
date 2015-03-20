package edu.uci.ics.asterix.external.library;

public class DocumentFeatureExtractionFunctionFactory implements IFunctionFactory {

    @Override
    public IExternalFunction getExternalFunction() {
        return new DocumentFeatureExtractionFunction();
    }

}
