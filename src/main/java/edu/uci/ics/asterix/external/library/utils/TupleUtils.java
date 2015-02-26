package edu.uci.ics.asterix.external.library.utils;

import java.util.*;
import java.util.Map.Entry;

import edu.uci.ics.asterix.external.library.java.IJObject;
import edu.uci.ics.asterix.external.library.java.JObjects.JRecord;
import edu.uci.ics.asterix.external.udl.adapter.factory.KBARecord;
import edu.uci.ics.asterix.om.types.ARecordType;

public class TupleUtils extends StringUtil {
    public static final String[] MAIN_FIELDS = { KBARecord.FIELD_STREAM_ID, KBARecord.FIELD_DIR_NAME,
            KBARecord.FIELD_SOURCE, KBARecord.FIELD_SCHOST, KBARecord.FIELD_TITLE, KBARecord.FIELD_ANCHOR,
            KBARecord.FIELD_LANGUAGE };

    @SuppressWarnings("unchecked")
    public static int getTupleSize(Map<String, Object> tupleMap) {
        Iterator<Object> it = tupleMap.values().iterator();

        int size = 0;
        while (it.hasNext()) {
            Object value = it.next();

            if (value instanceof String)
                size += sizeOfString((String) value);
            else if (value instanceof Collection) {
                Collection<String> valueElements = (Collection<String>) value;
                for (String e : valueElements) {
                    size += sizeOfString(e);
                }
            }
        }
        return size;
    }

    public static void copyMainFields(Map<String, Object> srcMap, Map<String, Object> dstMap) {
        for (String key : MAIN_FIELDS) {
            dstMap.put(key, (String) srcMap.get(key));
        }
    }

    public static class TupleInfo {
        String id;
        int bodySize;
        int sizeOtherFields;
        int numberOfSplits;
        int totalSize;
    }

    public static TupleInfo getTupleSizeInfo(Map<String, Object> tupleMap, int maxTupleSize) {
        tupleMap.entrySet();
        Iterator<Entry<String, Object>> it = tupleMap.entrySet().iterator();

        TupleInfo tupleInfo = new TupleInfo();
        tupleInfo.numberOfSplits = 1;
        tupleInfo.totalSize = 0;
        tupleInfo.sizeOtherFields = 0;

        while (it.hasNext()) {
            Entry<String, Object> e = it.next();
            Object value = e.getValue();
            int s = 0;

            if (value != null && value instanceof String)
                s = StringUtil.sizeOfString((String) value);

            else if (value instanceof Collection) {
                Collection<String> valueElements = (Collection<String>) value;
                for (String element : valueElements) {
                    s += sizeOfString(element);
                }
            }

            if (e.getKey().equals(KBARecord.FIELD_BODY)) {
                tupleInfo.bodySize = s;
            } else
                tupleInfo.sizeOtherFields += s;

            tupleInfo.totalSize += s;
        }
        tupleInfo.numberOfSplits = (int) Math.ceil(tupleInfo.totalSize / (double) maxTupleSize);

        return tupleInfo;
    }

    public static Object splitTuple(Map<String, Object> fields, int maxSize) {
        int tupleSize = getTupleSize(fields);

        if (tupleSize <= maxSize)
            return fields;

        return splitTuple(fields, tupleSize, maxSize);
    }
    
    public static int getFieldPosByName(JRecord jRecord, String fieldName) {
        ARecordType recordType = jRecord.getRecordType();
        return getFieldPosByName(recordType.getFieldNames(), fieldName);
    }
    
    public static int getFieldPosByName(String[] fieldNames, String fieldName) {
        int index = 0;
        for (String name : fieldNames) {
            if (name.equals(fieldName)) {
                return index;
            }
            index++;
        }
        return -1;
    }
    

    /**
     * Split a tuple (represented by the tupleMap) into smaller tuples to fit in the
     * frame size of AsterixDB
     * 
     * @param tupleMap
     *            the tuple as HashMap
     * @param tupleSize
     *            the current size of the tuple
     * @param maxSize
     *            the maximum allowed tuple size
     * @return
     *         Array of Maps containing the children tuples.
     */
    public static Map<String, Object>[] splitTuple(Map<String, Object> fields, int tupleSize, int maxSize) {
        int numSplits = (int) Math.ceil(tupleSize / (double) maxSize);

        // Split the body content into numSplits number of parts
        String bodyText = (String) fields.get(KBARecord.FIELD_BODY);
        int contentSplitSize = (int) Math.ceil(sizeOfString(bodyText) / (double) numSplits);
        String bodyTextSplits[] = breakString(bodyText, contentSplitSize);

        // Get smaller tuple splits
        @SuppressWarnings("unchecked")
        Map<String, Object> fieldsSplits[] = new Map[numSplits];
        String parentId = (String) fields.get(KBARecord.FIELD_DOCUMENT_ID);
        
        // Setup child-specific fields
        for (int part = 1; part < numSplits; part++) {
            fieldsSplits[part] = new HashMap<String, Object>();
            String docId = UUID.randomUUID().toString();
            fieldsSplits[part].put(KBARecord.FIELD_DOCUMENT_ID, docId);
            fieldsSplits[part].put(KBARecord.FIELD_PARENT, parentId);
            fieldsSplits[part].put(KBARecord.FIELD_BODY, bodyTextSplits[part]);
        }

        fields.put(KBARecord.FIELD_BODY, bodyTextSplits[0]);
        fieldsSplits[0] = fields;

        return fieldsSplits;
    }

}
