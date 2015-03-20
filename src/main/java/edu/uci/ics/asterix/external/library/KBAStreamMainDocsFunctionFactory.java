package edu.uci.ics.asterix.external.library;

public class KBAStreamMainDocsFunctionFactory implements IFunctionFactory {

    @Override
    public IExternalFunction getExternalFunction() {
        return new KBAStreamMainDocsFunction();
    }

}
