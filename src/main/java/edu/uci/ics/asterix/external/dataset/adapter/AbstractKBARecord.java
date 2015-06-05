package edu.uci.ics.asterix.external.dataset.adapter;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.trec.kba.streamcorpus.ContentItem;
import org.trec.kba.streamcorpus.Language;

import edu.uci.ics.asterix.external.library.utils.StringUtil;
import edu.uci.ics.asterix.external.library.utils.ADM.ADMArray;
import edu.uci.ics.asterix.external.library.utils.ADM.ADMObject;
import edu.uci.ics.asterix.external.library.utils.ADM.ADMOrderedArray;
import edu.uci.ics.asterix.external.library.utils.ADM.ADMUnorderedArray;
import edu.uci.ics.asterix.om.types.ATypeTag;

public abstract class AbstractKBARecord {
    protected final static int ASTERIX_STRING_LENGTH_LIMIT = 32745; // See UTF8StringWriter.java

    protected Set<String> mentionedEntities = null;

    protected HashMap<String, Object> fields;

    public final static String FIELD_DOCUMENT_ID = "doc_id";
    public final static String FIELD_STREAM_ID = "stream_id";
    public final static String FIELD_DIR_NAME = "dir_name";
    public final static String FIELD_SOURCE = "source";
    public final static String FIELD_SCHOST = "schost";
    public final static String FIELD_TITLE = "title_cleansed";
    public final static String FIELD_BODY = "body_cleansed";
    public final static String FIELD_ANCHOR = "anchor_cleansed";
    public final static String FIELD_LANGUAGE = "language";
    public final static String FIELD_MENTIONS = "mentions";
    public final static String FIELD_PARENT = "parent_id";
    public final static String FIELD_PART = "part_number";

    /**        
     * Field names and types + positions
     * doc_id: string,
     * stream_id: string,
     * title_cleansed: string,
     * body_cleansed: string,
     * source: string,
     * dir_name: string,
     * anchor_cleansed: string,
     * language: string,
     * schost: string
     * mentions: {{string}},
     * part_number: int32,
     * parent: string?
     */
    protected String[] fieldNames = { FIELD_DOCUMENT_ID, FIELD_STREAM_ID, FIELD_TITLE, FIELD_BODY, FIELD_SOURCE,
            FIELD_DIR_NAME, FIELD_ANCHOR, FIELD_LANGUAGE, FIELD_SCHOST };

    public final static String A_STRING_TYPE = "string";
    public final static String A_INT_TYPE = "int32";
    public final static String A_UNORDERED_STR_LIST = "{{string}}";
    public final static String A_ORDERED_STR_LIST = "[string]";

    protected enum KBAFields {
        FIELD_DOCUMENT_ID("doc_id", ATypeTag.STRING),
        FIELD_STREAM_ID("stream_id", ATypeTag.STRING),
        FIELD_TITLE("title_cleansed", ATypeTag.STRING),
        FIELD_BODY("body_cleansed", ATypeTag.ORDEREDLIST),
        FIELD_SOURCE("source", ATypeTag.STRING),
        FIELD_DIR_NAME("dir_name", ATypeTag.STRING),
        FIELD_ANCHOR("anchor_cleansed", ATypeTag.STRING),
        FIELD_LANGUAGE("language", ATypeTag.STRING),
        FIELD_SCHOST("schost", ATypeTag.STRING),
        FIELD_MENTIONS("mentions", ATypeTag.UNORDEREDLIST),
        FIELD_PART("part_number", ATypeTag.INT32),
        FIELD_PARENT("parent_id", ATypeTag.STRING);

        private static final Map<String, KBAFields> byName = new HashMap<String, KBAFields>();

        static {
            for (KBAFields field : EnumSet.allOf(KBAFields.class)) {
                byName.put(field.fieldName(), field);
            }
        }

        private static final String FIELD_NAMES[];
        static {
            int length = values().length;
            FIELD_NAMES = new String[length];
            int i = 0;
            for (KBAFields field : values()) {
                FIELD_NAMES[i] = field.fieldName;
            }
        }

        private String fieldName;
        private ATypeTag type;
        private int fieldPosition;

        KBAFields(String fieldName, ATypeTag type) {
            this.fieldName = fieldName;
            this.type = type;
            this.fieldPosition = this.ordinal();
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            KBAFields fields[] = values();
            sb.append(fields[0].fieldName + ":" + getTypeName(fields[0].type));
            for (int i = 1; i < fields.length; i++) {
                sb.append(",\n");
                sb.append(fields[i].fieldName + ":" + getTypeName(fields[i].type));
            }

            return sb.toString();
        }

        private String getTypeName(ATypeTag type) {
            switch (type) {
                case STRING:
                    return A_STRING_TYPE;
                case INT32:
                    return A_INT_TYPE;
                case UNORDEREDLIST:
                    return A_UNORDERED_STR_LIST;
                case ORDEREDLIST:
                    return A_ORDERED_STR_LIST;
                default:
                    return "";
            }
        }

        public String fieldName() {
            return this.fieldName;
        }

        public static int positionByName(String fieldName) {
            return byName.get(fieldName).fieldPosition;
        }

        public static ATypeTag getTypeByName(String fieldName) {
            return byName.get(fieldName).type;
        }

        public int position() {
            return this.fieldPosition;
        }

        public ATypeTag getType() {
            return this.type;
        }

        public static KBAFields getFieldByName(String fieldName) {
            return byName.get(fieldName);
        }
    }
    
    protected AbstractKBARecord() {
        this.fields = new HashMap<>();
    }
    

    protected String getLanguage(ContentItem ci) {
        Language language = ci.getLanguage();

        if (language == null) {
            return null;
        }

        if (language.isSetCode())
            return language.getCode();

        else if (language.isSetName())
            return language.getName();

        return null;
    }

    protected String getCleanVisible(ContentItem ci) {
        if (ci == null)
            return StringUtil.EMPTY_STRING;
        if (ci.getClean_visible() == null || ci.getClean_visible().length() == 0)
            return StringUtil.EMPTY_STRING;

        if (ci.getClean_visible().length() > 0) {
            return StringUtil.cleanText(ci.getClean_visible());
        } else {
            return StringUtil.EMPTY_STRING;
        }
    }

    public void setMentionedEntity(Set<String> entity_list) {
        this.mentionedEntities = entity_list;
    }

    public Set<String> getMentionedEntity() {
        return this.mentionedEntities;
    }

    public Object get(String fieldName) {
        return this.fields.get(fieldName);
    }

    /**
     * Generate the ADM equivalent string
     * 
     * @return the ADM string
     */
    public String toAdmEquivalent() {
        ADMObject adm_object = new ADMObject();
        ATypeTag aTypeTag;

        Iterator<Entry<String, Object>> it = fields.entrySet().iterator();
        ADMArray admArray = null;

        while (it.hasNext()) {
            Entry<String, Object> e = it.next();
            String key = e.getKey();
            Object value = e.getValue();

            aTypeTag = KBAFields.getTypeByName(key);
            if (value == null) {
                adm_object.put(key, "");
            } else {
                switch (aTypeTag) {
                    case INT32:
                        adm_object.put(key, Integer.toString((int) value));
                        break;
                    case UNORDEREDLIST:
                        admArray = new ADMUnorderedArray((Set<String>) value);
                        break;
                    case ORDEREDLIST:
                        if (key.equals(FIELD_BODY)) { // To deal with long string
                            admArray = new ADMOrderedArray(StringUtil.breakString((String) value,
                                    ASTERIX_STRING_LENGTH_LIMIT));
                            adm_object.put(key, admArray);
                        }
                        break;
                    default://STRING:
                        adm_object.put(key, (String) value);
                }
            }

        }
        return adm_object.toString();
    }

}
