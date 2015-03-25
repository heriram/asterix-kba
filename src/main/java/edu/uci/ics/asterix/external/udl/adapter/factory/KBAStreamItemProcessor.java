package edu.uci.ics.asterix.external.udl.adapter.factory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.uci.ics.asterix.external.library.utils.StringUtil;
import edu.uci.ics.asterix.om.base.AMutableInt16;
import edu.uci.ics.asterix.om.base.AMutableInt32;
import edu.uci.ics.asterix.om.base.AMutableOrderedList;
import edu.uci.ics.asterix.om.base.AMutableRecord;
import edu.uci.ics.asterix.om.base.AMutableString;
import edu.uci.ics.asterix.om.base.AMutableUnorderedList;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.AUnorderedListType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

public class KBAStreamItemProcessor {
    private final static int UTF8_STRING_SIZE_LIMIT = (int) (((65 * 1024) - 45) / 2);
    private IAObject[] mutableKBAStreamDocumentFields;
    private AMutableRecord mutableRecord;

    private ARecordType recordType;

    
    ArrayList<IAObject> bodyAsArrayList;
    
    
    public KBAStreamItemProcessor(ARecordType recordType, IHyracksTaskContext ctx) {
        this.recordType = recordType;
        IAType fieldTypes[] = recordType.getFieldTypes();
        
        this.bodyAsArrayList = new ArrayList<>();

        mutableKBAStreamDocumentFields = new IAObject[fieldTypes.length];
        int length = fieldTypes.length; //Math.max(fieldNames.length, fieldTypes.length);

        for (int i = 0; i < length; i++) {
            switch (fieldTypes[i].getTypeTag()) {
                case ORDEREDLIST: // For the body content
                    mutableKBAStreamDocumentFields[i] = new AMutableOrderedList((AOrderedListType) fieldTypes[i]);
                    break;

                case UNORDEREDLIST: // For the mentionlist
                    mutableKBAStreamDocumentFields[i] = new AMutableUnorderedList(new AUnorderedListType(
                            BuiltinType.ASTRING, "UNORDEREDLIST"));
                    break;
                case INT16:
                    mutableKBAStreamDocumentFields[i] = new AMutableInt16((short) 0);
                    break;
                case INT32:
                    mutableKBAStreamDocumentFields[i] = new AMutableInt32(0);
                    break;

                default:
                    mutableKBAStreamDocumentFields[i] = new AMutableString("");
                    break;
            }

        }

        mutableRecord = new AMutableRecord(recordType, mutableKBAStreamDocumentFields);
    }
    
    public void getBodyAMutableStringList(Map<String, Object> recordFields) {
        bodyAsArrayList.clear();
        
        String bodyText = ((String) recordFields.get(KBARecord.FIELD_BODY)).trim();
        int len = bodyText.length();
        int maxLen = UTF8_STRING_SIZE_LIMIT / 2;
        AMutableString mutableString = new AMutableString(null);

        // If short enough do nothing
        if (len <= maxLen) {
            mutableString.setValue(bodyText);
            bodyAsArrayList.add(mutableString);
            return;
        }

        // Otherwise, splitt into an OrderedList
        int beginIndex = 0;
        int endIndex = maxLen - 1;
        
        while (endIndex < len) {
            endIndex = Math.min(StringUtil.lastIndexOf(bodyText, beginIndex, endIndex, ' '), len - 1);
            mutableString.setValue(bodyText.substring(beginIndex, endIndex));
            bodyAsArrayList.add(mutableString);
            beginIndex = endIndex + 1;
            endIndex = beginIndex + maxLen - 2;
        }
        mutableString.setValue(bodyText.substring(beginIndex, len));
        bodyAsArrayList.add(mutableString);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void setListContent(int fieldPos, Map<String, Object> recordFields) {
        Set<String> mentions = (HashSet) recordFields.get(KBARecord.FIELD_MENTIONS);
        if (mentions != null) {
            ((AMutableUnorderedList) mutableKBAStreamDocumentFields[fieldPos]).clear();
            for (String mention : mentions) {
                ((AMutableUnorderedList) mutableKBAStreamDocumentFields[fieldPos]).add(new AMutableString(mention));
            }
        }
        mentions = null;
    }

    private void processMainDocument(String fieldNames[], Map<String, Object> recordFields) {
        IAType fieldTypes[] = recordType.getFieldTypes();

        for (int i = 0; i < fieldNames.length; i++) {

            switch (fieldTypes[i].getTypeTag()) {
                case ORDEREDLIST:
                    getBodyAMutableStringList(recordFields);
                    ((AMutableOrderedList) mutableKBAStreamDocumentFields[i]).setValues(bodyAsArrayList);
                    break;
                case UNORDEREDLIST:
                    setListContent(i, recordFields);
                    break;
                case INT32:
                    int fieldValue = 0;
                    Integer field = (Integer)recordFields.get(fieldNames[i]);
                    if (field!=null)
                        fieldValue = (int)field;
                    
                    ((AMutableInt32) mutableKBAStreamDocumentFields[i]).setValue(fieldValue);
                    break;
                default:
                    String fieldsValue = (String) recordFields.get(fieldNames[i]);
                    if (fieldsValue == null) {
                        fieldsValue = "";
                    }
                    ((AMutableString) mutableKBAStreamDocumentFields[i]).setValue(fieldsValue);
            }

            mutableRecord.setValueAtPos(i, mutableKBAStreamDocumentFields[i]);
        }
    }

    private void processChildDocument(String fieldNames[], Map<String, Object> recordFields) {
        IAType fieldTypes[] = recordType.getFieldTypes(); 
        for (int i = 0; i < fieldNames.length; i++) {
            if (fieldNames[i].equals(KBARecord.FIELD_DOCUMENT_ID)) {
                ((AMutableString) mutableKBAStreamDocumentFields[i]).setValue((String) recordFields
                        .get(KBARecord.FIELD_DOCUMENT_ID));
                mutableRecord.setValueAtPos(i, mutableKBAStreamDocumentFields[i]);
            } else if (fieldNames[i].equals(KBARecord.FIELD_BODY)) {
                getBodyAMutableStringList(recordFields);
                ((AMutableOrderedList) mutableKBAStreamDocumentFields[i]).setValues(bodyAsArrayList);
                mutableRecord.setValueAtPos(i, mutableKBAStreamDocumentFields[i]);
            } else if (fieldNames[i].equals(KBARecord.FIELD_PARENT)) {
                ((AMutableString) mutableKBAStreamDocumentFields[i]).setValue((String) recordFields
                        .get(KBARecord.FIELD_PARENT));
                mutableRecord.setValueAtPos(i, mutableKBAStreamDocumentFields[i]);
            } else if (fieldNames[i].equals(KBARecord.FIELD_PART)) {
                    ((AMutableInt32) mutableKBAStreamDocumentFields[i]).setValue((Integer) recordFields
                            .get(KBARecord.FIELD_PART));
                    mutableRecord.setValueAtPos(i, mutableKBAStreamDocumentFields[i]);
                
            } else {
                switch (fieldTypes[i].getTypeTag()) {
                    case UNORDEREDLIST: // For the mentionlist
                        mutableKBAStreamDocumentFields[i] = new AMutableUnorderedList(new AUnorderedListType(
                                BuiltinType.ASTRING, "UNORDEREDLIST"));
                        break;

                    default:
                        mutableKBAStreamDocumentFields[i] = new AMutableString("");
                        break;
                }
                mutableRecord.setValueAtPos(i, mutableKBAStreamDocumentFields[i]);
            }
        }

    }

    public AMutableRecord processNextStreamDocument(Map<String, Object> streamDocFields, String fieldNames[]) {
        if (!streamDocFields.containsKey(KBARecord.FIELD_PARENT)) {
            processMainDocument(fieldNames, streamDocFields);
        } else {
            processChildDocument(fieldNames, streamDocFields);
        }
        
        // Free the space
        streamDocFields = null;
    
        return mutableRecord;
    }

    public AMutableRecord getMutableRecord() {
        return mutableRecord;
    }

}