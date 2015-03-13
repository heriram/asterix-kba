package edu.uci.ics.asterix.external.library;

public class DocuementEntityFeatureExtractionFunctionFactory implements IFunctionFactory {

    @Override
    public IExternalFunction getExternalFunction() {
        return new DocumentEntityFeatureExtractionFunction();
    }

}
