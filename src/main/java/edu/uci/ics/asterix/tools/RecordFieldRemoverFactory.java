package edu.uci.ics.asterix.tools;

import edu.uci.ics.asterix.external.library.IExternalFunction;
import edu.uci.ics.asterix.external.library.IFunctionFactory;

public class RecordFieldRemoverFactory implements IFunctionFactory{

    @Override
    public IExternalFunction getExternalFunction() {
        return new RecordFieldRemover();
    }

}
