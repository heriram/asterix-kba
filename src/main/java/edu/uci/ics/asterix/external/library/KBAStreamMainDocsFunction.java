package edu.uci.ics.asterix.external.library;

import java.util.logging.Logger;

import edu.uci.ics.asterix.external.library.java.IJObject;
import edu.uci.ics.asterix.external.library.java.JObjects.JRecord;
import edu.uci.ics.asterix.external.library.java.JObjects.JString;

public class KBAStreamMainDocsFunction  implements IExternalScalarFunction  {
    private static final Logger LOGGER = Logger.getLogger(KBAStreamMainDocsFunction.class.getName());

    public KBAStreamMainDocsFunction() {
        LOGGER.info("Instantiating " + KBAStreamMainDocsFunction.class.getName());
    }

    @Override
    public void deinitialize() {
        // TODO Auto-generated method stub

    }

    @Override
    public void initialize(IFunctionHelper functionHelper) throws Exception {
        // TODO Auto-generated method stub
        LOGGER.info("Initializing... KBAStreamMainDocsFunction");
    }


    @Override
    public void evaluate(IFunctionHelper functionHelper) throws Exception {
        // Get the input
        JRecord inputRecord = (JRecord) functionHelper.getArgument(0);
        
        LOGGER.info("Evaluating...nothing...");

        JString parentId = (JString)inputRecord.getValueByName("parent");
        if (parentId == null) {
         // Get the fields
            IJObject[] fields = inputRecord.getFields();
            JRecord result = (JRecord) functionHelper.getResultObject();
            for (int i=0; i<9; i++) {
                result.setValueAtPos(i, fields[i]);
            }
            functionHelper.setResult(result);  
        }
    }
}
