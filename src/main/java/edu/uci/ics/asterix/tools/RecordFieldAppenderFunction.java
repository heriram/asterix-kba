package edu.uci.ics.asterix.tools;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import edu.uci.ics.asterix.external.library.IExternalScalarFunction;
import edu.uci.ics.asterix.external.library.IFunctionHelper;
import edu.uci.ics.asterix.external.library.java.IJObject;
import edu.uci.ics.asterix.external.library.java.JObjects.JRecord;
import edu.uci.ics.asterix.om.types.ARecordType;

public class RecordFieldAppenderFunction implements IExternalScalarFunction {
    private final int INPUT_RECORD = 0;
    private final int INPUT_ARG = 1;

    private Set<String> outputFieldSet;
    private List<String> inputFieldNames;
    private List<IJObject> inputFields;

    @Override
    public void initialize(IFunctionHelper functionHelper) throws Exception {
        outputFieldSet = new HashSet<String>();
        inputFields = new ArrayList<IJObject>();
        inputFieldNames = new ArrayList<String>();
    }

    @Override
    public void deinitialize() {
        // TODO Auto-generated method stub

    }

    private void addOutputFieldNames(String fieldNames[]) {
        outputFieldSet.clear();
        for (String fieldName : fieldNames)
            outputFieldSet.add(fieldName);
    }

    private void initInputFields(JRecord inputRecord, JRecord appendRecord) {
        inputFields.clear();

        // Get the fields from input record
        IJObject[] fields = inputRecord.getFields();
        String fieldNames[] = inputRecord.getRecordType().getFieldNames();
        for (int i = 0; i < fieldNames.length; i++) {
            inputFieldNames.add(fieldNames[i]);
            inputFields.add(fields[i]);
        }

        Map<String, IJObject> inputRecOpenFields = inputRecord.getOpenFields();
        inputFieldNames.addAll(inputRecOpenFields.keySet());
        inputFields.addAll(inputRecOpenFields.values());

        // Get the fields from the record to be appended
        IJObject[] appendFields = appendRecord.getFields();
        String appendFieldNames[] = appendRecord.getRecordType().getFieldNames();
        for (int i = 0; i < appendFieldNames.length; i++) {
            inputFieldNames.add(appendFieldNames[i]);
            inputFields.add(appendFields[i]);
        }

        Map<String, IJObject> appendRecOpenFields = appendRecord.getOpenFields();
        inputFieldNames.addAll(appendRecOpenFields.keySet());
        inputFields.addAll(appendRecOpenFields.values());
    }

    @Override
    public void evaluate(IFunctionHelper functionHelper) throws Exception {

        JRecord inputRecord = (JRecord) functionHelper.getArgument(INPUT_RECORD);
        JRecord appendRecord = (JRecord) functionHelper.getArgument(INPUT_ARG);

        initInputFields(inputRecord, appendRecord);

        JRecord result = (JRecord) functionHelper.getResultObject();
        addOutputFieldNames(result.getRecordType().getFieldNames());

        for (int i = 0; i < inputFieldNames.size(); i++) {
            String fieldName = inputFieldNames.get(i);
            if (outputFieldSet.contains(fieldName))
                result.setField(fieldName, inputFields.get(i));
            else
                result.addField(fieldName, inputFields.get(i));
        }

        functionHelper.setResult(result);

    }

}
