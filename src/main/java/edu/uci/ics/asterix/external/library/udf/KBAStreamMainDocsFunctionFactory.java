package edu.uci.ics.asterix.external.library.udf;

import edu.uci.ics.asterix.external.library.IExternalFunction;
import edu.uci.ics.asterix.external.library.IFunctionFactory;

public class KBAStreamMainDocsFunctionFactory implements IFunctionFactory {

    @Override
    public IExternalFunction getExternalFunction() {
        return new KBAStreamMainDocsFunction();
    }

}
