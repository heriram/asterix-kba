package edu.uci.ics.asterix.external.dataset.adapter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.uci.ics.asterix.external.dataset.adapter.AbstractKBARecord.KBAFields;
import edu.uci.ics.asterix.external.library.AMutableObjectFactory;
import edu.uci.ics.asterix.external.library.JTypeObjectFactory;
import edu.uci.ics.asterix.external.library.java.IJObject;
import edu.uci.ics.asterix.external.library.java.JTypeTag;
import edu.uci.ics.asterix.external.library.java.JObjects.JNull;
import edu.uci.ics.asterix.external.library.utils.StringUtil;
import edu.uci.ics.asterix.om.base.AMutableInt16;
import edu.uci.ics.asterix.om.base.AMutableInt32;
import edu.uci.ics.asterix.om.base.AMutableOrderedList;
import edu.uci.ics.asterix.om.base.AMutableRecord;
import edu.uci.ics.asterix.om.base.AMutableString;
import edu.uci.ics.asterix.om.base.AMutableUnorderedList;
import edu.uci.ics.asterix.om.base.IACollection;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.AUnorderedListType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.container.IObjectPool;
import edu.uci.ics.asterix.om.util.container.ListObjectPool;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

public class KBAStreamItemProcessor {
    private final static int UTF8_STRING_SIZE_LIMIT = (int) (((65 * 1024) - 45) / 2);
    private IAObject[] mutableKBAStreamDocumentFields;
    private AMutableRecord mutableRecord;

    private final IObjectPool<IAObject, IAType> objectPool = new ListObjectPool<IAObject, IAType>(
            AMutableObjectFactory.INSTANCE);

    private ARecordType recordType;
    
    private Map<String, Integer> positionMap;


    ArrayList<IAObject> bodyAsArrayList;

    public KBAStreamItemProcessor(ARecordType recordType, IHyracksTaskContext ctx) {
        this.recordType = recordType;
        IAType fieldTypes[] = recordType.getFieldTypes();
        
        this.positionMap = new HashMap<>();
        String fieldNames[] = recordType.getFieldNames();

        this.bodyAsArrayList = new ArrayList<>();

        mutableKBAStreamDocumentFields = new IAObject[fieldTypes.length];
        int length = fieldTypes.length; //Math.max(fieldNames.length, fieldTypes.length);

        for (int i = 0; i < length; i++) {
            positionMap.put(fieldNames[i], i);
            
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

    private AMutableString allocateString(String value) {
        AMutableString retValue = (AMutableString) objectPool.allocate(BuiltinType.ASTRING);
        retValue.setValue(value);

        return retValue;
    }

    public void reset() {
        for (int i = 0; i < mutableKBAStreamDocumentFields.length; i++) {
            switch (mutableKBAStreamDocumentFields[i].getType().getTypeTag()) {
                case ORDEREDLIST: // For the body content
                    bodyAsArrayList.clear();
                    break;
                case UNORDEREDLIST: // For the mentionlist
                    ((AMutableUnorderedList) mutableKBAStreamDocumentFields[i]).clear();
                    break;
                case STRING:
                    ((AMutableString) mutableKBAStreamDocumentFields[i]).setValue("");
                    break;
                default:
                    break;
            }
        }

        objectPool.reset();
    }

    public void getBodyAMutableStringList(Map<String, Object> recordFields) {
        AMutableString aString = null;
        String bodyText = ((String) recordFields.get(KBARecord.FIELD_BODY)).trim();
        int len = bodyText.length();
        int maxLen = UTF8_STRING_SIZE_LIMIT / 2;

        bodyAsArrayList.clear();

        // If short enough do nothing
        if (len <= maxLen) {
            aString = allocateString(bodyText);
            bodyAsArrayList.add(aString);
            return;
        }

        // Otherwise, splitt into an OrderedList
        int beginIndex = 0;
        int endIndex = maxLen - 1;

        while (endIndex < len) {
            endIndex = Math.min(StringUtil.lastIndexOf(bodyText, beginIndex, endIndex, ' '), len - 1);
            aString = allocateString(bodyText.substring(beginIndex, endIndex));
            bodyAsArrayList.add(aString);

            beginIndex = endIndex + 1;
            endIndex = beginIndex + maxLen - 2;
        }

        aString = allocateString(bodyText.substring(beginIndex, len));
        bodyAsArrayList.add(aString);
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
        mentions.clear();
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
                    Integer field = (Integer) recordFields.get(fieldNames[i]);
                    if (field != null)
                        fieldValue = (int) field;

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

    private void processChildDocument(Map<String, Object> recordFields) {
        IAType fieldTypes[] = recordType.getFieldTypes();
       
        Object fieldValue = null;

        Set<String> recordFieldNames = recordFields.keySet();

        for (String fieldName : recordFieldNames) {         
            int pos = positionMap.get(fieldName);           
            fieldValue = recordFields.get(fieldName);
           
            ATypeTag typeTag = fieldTypes[pos].getTypeTag();
            switch (typeTag) {
                case STRING:
                    ((AMutableString) mutableKBAStreamDocumentFields[pos]).setValue((String) fieldValue);
                    break;
                case ORDEREDLIST: // Assumes FIELD_BODY
                    getBodyAMutableStringList(recordFields);
                    ((AMutableOrderedList) mutableKBAStreamDocumentFields[pos]).setValues(bodyAsArrayList);
                    break;
                case INT32: // Assumes FIELD_PART
                    ((AMutableInt32) mutableKBAStreamDocumentFields[pos]).setValue((Integer) fieldValue);
                    break;
                case UNORDEREDLIST: // Assumes FIELD_MENTIONS
                    mutableKBAStreamDocumentFields[pos] = new AMutableUnorderedList(new AUnorderedListType(
                            BuiltinType.ASTRING, "UNORDEREDLIST"));
                    break;
                case UNION:
                    if (fieldName.equals(KBARecord.FIELD_PARENT)) {
                        ((AMutableString) mutableKBAStreamDocumentFields[pos]).setValue((String) fieldValue);
                        break;
                    }
                default:
                    try {
                        throw new Exception("Unsupported field type: " + typeTag);
                    } catch (Exception e) {
                        e.printStackTrace();
                    } 
            }
            mutableRecord.setValueAtPos(pos, mutableKBAStreamDocumentFields[pos]);
        }

    }

    public AMutableRecord processNextStreamDocument(Map<String, Object> streamDocFields, String fieldNames[]) {
        // Reset the mutable record.
        reset();

        if (!streamDocFields.containsKey(KBARecord.FIELD_PARENT)) {
            processMainDocument(fieldNames, streamDocFields);
        } else {
            processChildDocument(streamDocFields);
        }

        // Free the space
        streamDocFields.clear();

        return mutableRecord;
    }

    public AMutableRecord getMutableRecord() {
        return mutableRecord;
    }

}