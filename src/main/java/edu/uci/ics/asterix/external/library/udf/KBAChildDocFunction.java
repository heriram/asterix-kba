package edu.uci.ics.asterix.external.library.udf;

import java.util.logging.Logger;

import edu.uci.ics.asterix.external.library.IExternalScalarFunction;
import edu.uci.ics.asterix.external.library.IFunctionHelper;
import edu.uci.ics.asterix.external.library.java.IJObject;
import edu.uci.ics.asterix.external.library.java.JObjects.JRecord;
import edu.uci.ics.asterix.external.library.java.JObjects.JString;
import edu.uci.ics.asterix.external.library.utils.TupleUtils;
import edu.uci.ics.asterix.external.udl.adapter.factory.KBARecord;

public class KBAChildDocFunction implements IExternalScalarFunction {
    private static final Logger LOGGER = Logger.getLogger(KBAChildDocFunction.class.getName());

    private int docIdPosition;
    private int parentIdPosition;
    private int bodyTextPosition;
    private int partNumberPosition = 0;

    public KBAChildDocFunction() {
        LOGGER.info("Instantiating KBAChildDocFunction");
    }

    @Override
    public void deinitialize() {
        // TODO Auto-generated method stub

    }

    @Override
    public void initialize(IFunctionHelper functionHelper) throws Exception {
        LOGGER.info("Initializing... KBAChildDocFunction");
        @SuppressWarnings("unused")
        JRecord inputRecord = (JRecord) functionHelper.getArgument(0);
        this.docIdPosition = TupleUtils.getFieldPosByName(inputRecord, KBARecord.FIELD_DOCUMENT_ID);
        this.parentIdPosition = TupleUtils.getFieldPosByName(inputRecord, KBARecord.FIELD_PARENT);
        this.bodyTextPosition = TupleUtils.getFieldPosByName(inputRecord, KBARecord.FIELD_BODY);
        
        int pnp = TupleUtils.getFieldPosByName(inputRecord, KBARecord.FIELD_PART);
        if (pnp>-1)
            this.partNumberPosition = pnp;
    }

    private boolean isAChildDocument(IJObject fields[]) {
        // Contain parent field?
        if (parentIdPosition == -1) 
            return false;
        
        if (fields[parentIdPosition] == null)
            return false;

        String parent = ((JString) fields[parentIdPosition]).getValue();
        
        return (parent != null && !parent.isEmpty());
    }


    @Override
    public void evaluate(IFunctionHelper functionHelper) throws Exception {
        // Get the input
        JRecord inputRecord = (JRecord) functionHelper.getArgument(0);

        // Get the fields
        IJObject[] fields = inputRecord.getFields();

        JRecord result = (JRecord) functionHelper.getResultObject();
        result.setField("doc_id", fields[docIdPosition]);
        result.setField("body_cleansed", fields[bodyTextPosition]);
        result.setField("part_number", fields[partNumberPosition]);
        if (isAChildDocument(fields)) {
            result.setField("parent_id", fields[parentIdPosition]);
            LOGGER.info("Trying to insert: " + ((JString) fields[docIdPosition]).getValue());
        } 
        
        functionHelper.setResult(result);
           
    }

}
