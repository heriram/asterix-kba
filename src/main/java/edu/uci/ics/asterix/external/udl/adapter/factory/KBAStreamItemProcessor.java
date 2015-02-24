package edu.uci.ics.asterix.external.udl.adapter.factory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import edu.uci.ics.asterix.external.library.utils.StringUtil;
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

public class KBAStreamItemProcessor /* extends KBAStreamItem */{
    private final static int UTF8_STRING_SIZE_LIMIT = (int) (((65 * 1024) - 45) / 2);
    private int maxTupleSize = Integer.MAX_VALUE;
    private int maxBodyTextSize = Integer.MAX_VALUE;

    private IAObject[] mutableKBAStreamDocumentFields;
    private AMutableRecord mutableRecord;
    private int bodeFieldIndex = 3;
    private IAType fieldTypes[];

    private Map<String, Object> fields;

    public KBAStreamItemProcessor(ARecordType recordType, IHyracksTaskContext ctx) {
        this.maxTupleSize = ctx.getFrameSize();

        this.fieldTypes = recordType.getFieldTypes();

        mutableKBAStreamDocumentFields = new IAObject[fieldTypes.length];
        int length = fieldTypes.length; //Math.max(fieldNames.length, fieldTypes.length);

        for (int i = 0; i < length; i++) {
            switch (fieldTypes[i].getTypeTag()) {
                case ORDEREDLIST: // For the body content
                    mutableKBAStreamDocumentFields[i] = new AMutableOrderedList((AOrderedListType) fieldTypes[i]);
                    bodeFieldIndex = i;
                    break;
                case UNION: // mentionlist is an optional field
                case UNORDEREDLIST: // For the mentionlist
                    //if (i==fieldNames.length)
                    mutableKBAStreamDocumentFields[i] = new AMutableUnorderedList(new AUnorderedListType(
                            BuiltinType.ASTRING, "UNORDEREDLIST"));
                    break;
                case STRING:
                    mutableKBAStreamDocumentFields[i] = new AMutableString("");
                    break;
                default:

            }

        }

        mutableRecord = new AMutableRecord(recordType, mutableKBAStreamDocumentFields);
    }

    private int getNumberOfFittingTuples() {
        int num = 1;
        fields.entrySet();
        int totalSize = 0;
        int otherFieldSize = 0;
        Iterator<Entry<String, Object>> it = fields.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, Object> e = it.next();
            String value = (String) e.getValue();
            int s = 0;
            if (value != null)
                s = StringUtil.sizeOfString(value);
            if (!e.getKey().equals(KBARecord.FIELD_BODY)) {
                otherFieldSize += s;
            }
            totalSize += s;
        }
        num = (int) Math.ceil(totalSize / (double) maxTupleSize);

        maxBodyTextSize = maxTupleSize - otherFieldSize;
        return num;
    }

    public ArrayList<IAObject> getBodyAMutableStringList() {
        ArrayList<IAObject> bodyAsArrayList = new ArrayList<>();
        String bodyText = ((String) fields.get(KBARecord.FIELD_BODY)).trim();
        int len = bodyText.length();
        int maxLen = UTF8_STRING_SIZE_LIMIT / 2;

        // If short enough do nothing
        if (len <= maxLen) {
            bodyAsArrayList.add(new AMutableString(bodyText));
            return bodyAsArrayList;
        }

        // Otherwise, splitt into an OrderedList
        int beginIndex = 0;
        int endIndex = maxLen - 1;
        IAObject subString;
        int i = 0;
        while (endIndex < len && i < 2) {
            endIndex = Math.min(StringUtil.lastIndexOf(bodyText, beginIndex, endIndex, ' '), len - 1);
            subString = new AMutableString(bodyText.substring(beginIndex, endIndex));
            bodyAsArrayList.add(subString);
            beginIndex = endIndex + 1;
            endIndex = beginIndex + maxLen - 2;
            i++;
        }
        //subString = new AMutableString(bodyText.substring(beginIndex, len));
        //bodyAsArrayList.add(subString);

        return bodyAsArrayList;
    }

    private void setListContent(int fieldPos) {
        Set<String> mentions = (HashSet) fields.get(KBARecord.FIELD_MENTIONS);
        if (mentions != null) {
            ((AMutableUnorderedList) mutableKBAStreamDocumentFields[fieldPos]).clear();
            for (String mention : mentions) {
                ((AMutableUnorderedList) mutableKBAStreamDocumentFields[fieldPos]).add(new AMutableString(mention));
            }
        }
        mentions.clear();
    }

    public AMutableRecord processNextStreamDocument(Map<String, Object> streamDocFields, String fieldNames[]) {
        this.fields = streamDocFields;

        //if (getNumberOfFittingTuples() <= 1) {
        for (int i = 0; i < fieldNames.length; i++) {

            switch (fieldTypes[i].getTypeTag()) {
                case ORDEREDLIST:
                    ((AMutableOrderedList) mutableKBAStreamDocumentFields[i]).setValues(getBodyAMutableStringList());
                    break;
                case UNORDEREDLIST:
                    setListContent(i);
                    break;
                default:
                    String fieldsValue = (String) fields.get(fieldNames[i]);
                    if (fieldsValue == null) {
                        fieldsValue = "";
                    }
                    ((AMutableString) mutableKBAStreamDocumentFields[i]).setValue(fieldsValue);
            }

            mutableRecord.setValueAtPos(i, mutableKBAStreamDocumentFields[i]);
        }

        fields.clear();
        return mutableRecord;

    }

    public AMutableRecord getMutableRecord() {
        return mutableRecord;
    }

}