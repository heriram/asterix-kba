package edu.uci.ics.asterix.external.library.udf;

import edu.uci.ics.asterix.external.library.IExternalFunction;
import edu.uci.ics.asterix.external.library.IFunctionFactory;


public class KBAStreamFilteringFunctionFactory implements IFunctionFactory {

    @Override
    public IExternalFunction getExternalFunction() {
        return new KBAStreamFilteringFunction();
    }

}
