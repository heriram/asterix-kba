package edu.uci.ics.asterix.tools;

import java.util.Map;

import edu.uci.ics.asterix.external.library.IExternalScalarFunction;
import edu.uci.ics.asterix.external.library.IFunctionHelper;
import edu.uci.ics.asterix.external.library.java.IJObject;
import edu.uci.ics.asterix.external.library.java.JObjects.JOrderedList;
import edu.uci.ics.asterix.external.library.java.JTypeTag;
import edu.uci.ics.asterix.external.library.java.JObjects.JRecord;
import edu.uci.ics.asterix.external.library.java.JObjects.JString;
import edu.uci.ics.asterix.external.library.java.JObjects.JUnorderedList;

public class RecordDebugFunction implements IExternalScalarFunction {
    private final int INPUT_RECORD = 0;
    
    private JOrderedList closedFieldList = null;
    private JOrderedList openFieldList = null;
    

    @Override
    public void deinitialize() {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void initialize(IFunctionHelper functionHelper) throws Exception {
        closedFieldList = new JOrderedList(functionHelper.getObject(JTypeTag.STRING));
        openFieldList = new JOrderedList(functionHelper.getObject(JTypeTag.STRING));
        
    }

    @Override
    public void evaluate(IFunctionHelper functionHelper) throws Exception {
        closedFieldList.clear();
        openFieldList.clear();
        
        JRecord inputRecord = (JRecord) functionHelper.getArgument(INPUT_RECORD);
        String fieldNames[] = inputRecord.getRecordType().getFieldNames();
          
        JRecord result = (JRecord) functionHelper.getResultObject();
        IJObject[] fields = inputRecord.getFields();
        for (int i=0; i<fields.length; i++) {
            JString newField = (JString) functionHelper.getObject(JTypeTag.STRING);
            newField.setValue(fieldNames[i]);
            closedFieldList.add(newField);  
           
        }
        result.setField("closed_fields", closedFieldList);
        
        // Get the open fields as well.
        Map<String, IJObject> inputRecOpenFields = inputRecord.getOpenFields();
        for (String fieldName: inputRecOpenFields.keySet()) {
            JString newField = (JString) functionHelper.getObject(JTypeTag.STRING);
            newField.setValue(fieldName);
            openFieldList.add(newField);  
        }
        result.setField("open_fields", openFieldList);
        
        functionHelper.setResult(result);
        
    }

}
