package edu.uci.ics.asterix.external.library;


public class KBAStreamFilteringFunctionFactory implements IFunctionFactory {

    @Override
    public IExternalFunction getExternalFunction() {
        return new KBAStreamFilteringFunction();
    }

}
