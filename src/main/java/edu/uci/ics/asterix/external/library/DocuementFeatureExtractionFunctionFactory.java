package edu.uci.ics.asterix.external.library;

public class DocuementFeatureExtractionFunctionFactory implements IFunctionFactory {

    @Override
    public IExternalFunction getExternalFunction() {
        return new DocumentFeatureExtractionFunction();
    }

}
