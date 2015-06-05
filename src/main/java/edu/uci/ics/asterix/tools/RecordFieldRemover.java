package edu.uci.ics.asterix.tools;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.uci.ics.asterix.external.library.IExternalScalarFunction;
import edu.uci.ics.asterix.external.library.IFunctionHelper;
import edu.uci.ics.asterix.external.library.java.IJObject;
import edu.uci.ics.asterix.external.library.java.JObjects.JOrderedList;
import edu.uci.ics.asterix.external.library.java.JObjects.JRecord;
import edu.uci.ics.asterix.external.library.java.JObjects.JString;

public class RecordFieldRemover implements IExternalScalarFunction {
    private final int INPUT_RECORD = 0;
    private final int INPUT_ARG = 1;
    
    private Set<String> fieldSetToRemove;

    @Override
    public void initialize(IFunctionHelper functionHelper) throws Exception {
        fieldSetToRemove = new HashSet<String>();
    }
    
    private void getFieldNames(IFunctionHelper functionHelper) {
        JRecord fieldsToRemove = (JRecord) functionHelper.getArgument(INPUT_ARG);
        JOrderedList fieldNameList = (JOrderedList) fieldsToRemove.getFields()[0];
        for (int i=0; i<fieldNameList.size(); i++) {
            JString fieldName = (JString) fieldNameList.getElement(i);
            fieldSetToRemove.add(fieldName.getValue());
        }
    }

    @Override
    public void deinitialize() {
    }

    @Override
    public void evaluate(IFunctionHelper functionHelper) throws Exception {
        fieldSetToRemove.clear();
        getFieldNames(functionHelper);
        
        JRecord inputRecord = (JRecord) functionHelper.getArgument(INPUT_RECORD);
        String fieldNames[] = inputRecord.getRecordType().getFieldNames();
        
      
        JRecord result = (JRecord) functionHelper.getResultObject();
        IJObject[] fields = inputRecord.getFields();
        for (int i=0; i<fields.length; i++) {
            String fieldName = fieldNames[i];
            if (!fieldSetToRemove.contains(fieldName))
                result.addField(fieldNames[i], fields[i]);
           
        }
        
        // Get the open fields as well.
        Map<String, IJObject> inputRecOpenFields = inputRecord.getOpenFields();
        for (String fieldName: inputRecOpenFields.keySet()) {
            if (!fieldSetToRemove.contains(fieldName))
                result.addField(fieldName, inputRecOpenFields.get(fieldName));
        }
            
     // TODO Add also debug format record...
        functionHelper.setResult(result);
    }

}