package edu.uci.ics.asterix.external.library.utils;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;

import edu.uci.ics.asterix.external.dataset.adapter.KBARecord;
import edu.uci.ics.asterix.external.library.java.JObjects.JRecord;
import edu.uci.ics.asterix.om.types.ARecordType;

public class TupleUtils extends StringUtil {
    public static final String[] MAIN_FIELDS = { KBARecord.FIELD_STREAM_ID, KBARecord.FIELD_DIR_NAME,
            KBARecord.FIELD_SOURCE, KBARecord.FIELD_SCHOST, KBARecord.FIELD_TITLE, KBARecord.FIELD_ANCHOR,
            KBARecord.FIELD_LANGUAGE };

    // Make sure this class is never instantiated
    private TupleUtils() {

    }

    /**
     * Compute the size of a tuple to figure out whether it will fit into
     * the Hyracs frame
     * 
     * @param tupleAsMap
     *            The {@link HashMap} representation of the tuple
     * @return
     *         The size of the tuple (int)
     */
    @SuppressWarnings("unchecked")
    public static int getTupleSize(Map<String, Object> tupleAsMap) {
        Iterator<Object> it = tupleAsMap.values().iterator();

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

        TupleInfo ti = new TupleInfo();
        ti.id = (String) tupleMap.get(KBARecord.FIELD_DOCUMENT_ID);
        ti.numberOfSplits = 1;
        ti.totalSize = 0;
        ti.sizeOtherFields = 0;

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
                ti.bodySize = s;
            } else
                ti.sizeOtherFields += s;

            ti.totalSize += s;
        }
        ti.numberOfSplits = (int) Math.ceil(ti.totalSize / (double) maxTupleSize);

        return ti;
    }

    public static Object splitTuple(Map<String, Object> fields, int maxSize) {
        int tupleSize = getTupleSize(fields);

        if (tupleSize <= maxSize)
            return fields;

        return splitTuple(fields, tupleSize, maxSize);
    }

    public static void splitTuple(Map<String, Object> fields, int tupleSize, int maxTupleSize,
            BlockingQueue<Map<String, Object>> dataInputQueue) {
        if (dataInputQueue != null) {
            // Get the min. number of splits
            int numSplits = (int) Math.ceil(tupleSize / (double) maxTupleSize);
            Map<String, Object> splitMap[] = getTupleSplits(fields, getBodyFieldSplits(fields, numSplits));
            for (Map<String, Object> tuple : splitMap) {
                dataInputQueue.add(tuple);
            }
        }
    }

    public static String[] getBodyFieldSplits(Map<String, Object> fields, int minTupleSplits) {
        // Split the body content into numSplits number of parts
        String bodyText = (String) fields.get(KBARecord.FIELD_BODY);

        // Get max size of the body text in each split
        int contentSplitSize = (int) Math.ceil(bodyText.length() / (double) minTupleSplits);

        return breakString(bodyText, contentSplitSize);
    }

    public static Map<String, Object>[] getTupleSplits(Map<String, Object> fields, String bodyTextSplits[]) {
        String parentId = (String) fields.get(KBARecord.FIELD_DOCUMENT_ID);
        try {
            if (parentId == null)
                throw new Exception("Document ID cannot be null");

            if (parentId.isEmpty())
                throw new Exception("Document ID cannot be empty");
        } catch (Exception e) {
            e.printStackTrace();
        }

        int numSplits = bodyTextSplits.length;

        // Get smaller tuple splits
        @SuppressWarnings("unchecked")
        Map<String, Object> fieldsSplits[] = new Map[numSplits];

        // Setup child-specific fields
        for (int part = 1; part < numSplits; part++) {
            fieldsSplits[part] = new HashMap<String, Object>();
            UUID uuid = UUID.randomUUID();
            fieldsSplits[part].put(KBARecord.FIELD_DOCUMENT_ID, uuid.toString());
            fieldsSplits[part].put(KBARecord.FIELD_PARENT, parentId);
            fieldsSplits[part].put(KBARecord.FIELD_BODY, bodyTextSplits[part]);
            fieldsSplits[part].put(KBARecord.FIELD_PART, part);
        }

        // Update the parent's field values
        fields.put(KBARecord.FIELD_BODY, bodyTextSplits[0]);
        fields.put(KBARecord.FIELD_PART, 0);
        fieldsSplits[0] = fields;

        return fieldsSplits;
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
        // Get the min number of splits
        int numSplits = (int) Math.ceil(tupleSize / (double) maxSize);

        String parentId = (String) fields.get(KBARecord.FIELD_DOCUMENT_ID);

        // Split the body content into numSplits number of parts
        String bodyText = (String) fields.get(KBARecord.FIELD_BODY);

        // Get max size of the body text in each split
        int contentSplitSize = (int) Math.ceil(bodyText.length() / (double) numSplits);

        String bodyTextSplits[] = breakString(bodyText, contentSplitSize);
        numSplits = bodyTextSplits.length;

        // Get smaller tuple splits
        @SuppressWarnings("unchecked")
        Map<String, Object> fieldsSplits[] = new Map[numSplits];

        // Setup child-specific fields
        for (int part = 1; part < numSplits; part++) {
            fieldsSplits[part] = new HashMap<String, Object>();
            String docId = UUID.randomUUID().toString();
            fieldsSplits[part].put(KBARecord.FIELD_DOCUMENT_ID, docId);
            fieldsSplits[part].put(KBARecord.FIELD_PARENT, parentId);
            fieldsSplits[part].put(KBARecord.FIELD_BODY, bodyTextSplits[part]);
            fieldsSplits[part].put(KBARecord.FIELD_PART, part);
        }

        fields.put(KBARecord.FIELD_BODY, bodyTextSplits[0]);
        fields.put(KBARecord.FIELD_PART, 0);
        fieldsSplits[0] = fields;

        return fieldsSplits;
    }

}
