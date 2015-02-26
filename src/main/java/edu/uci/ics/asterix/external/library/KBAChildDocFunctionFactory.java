package edu.uci.ics.asterix.external.library;

public class KBAChildDocFunctionFactory implements IFunctionFactory {

    @Override
    public IExternalFunction getExternalFunction() {
        return new KBAChildDocFunction();
    }

}