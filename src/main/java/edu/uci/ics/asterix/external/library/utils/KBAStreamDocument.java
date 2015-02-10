package edu.uci.ics.asterix.external.library.utils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.trec.kba.streamcorpus.ContentItem;
import org.trec.kba.streamcorpus.Language;
import org.trec.kba.streamcorpus.StreamItem;

import edu.uci.ics.asterix.external.library.utils.ADM.ADMArray;
import edu.uci.ics.asterix.external.library.utils.ADM.ADMObject;
import edu.uci.ics.asterix.external.library.utils.ADM.ADMOrderedArray;

public class KBAStreamDocument {

    protected final static int ASTERIX_STRING_LENGTH_LIMIT = 32745; // See UTF8StringWriter.java

    List<String> mentionedEntities = null;

    protected HashMap<String, String> fields;

    public final static String FIELD_DOCUMENT_ID = "doc_id";
    public final static String FIELD_STREAM_ID = "stream_id";
    public final static String FIELD_DIR_NAME = "dir_name";
    public final static String FIELD_SOURCE = "source";
    public final static String FIELD_SCHOST = "schost";
    public final static String FIELD_TITLE = "title_cleansed";
    public final static String FIELD_BODY = "body_cleansed";
    public final static String FIELD_ANCHOR = "anchor_cleansed";
    public final static String FIELD_LANGUAGE = "language";

    /**
     * Field names
     */
    protected static String[] fieldNames = { FIELD_DOCUMENT_ID, FIELD_STREAM_ID, FIELD_DIR_NAME, FIELD_SOURCE,
            FIELD_SCHOST, FIELD_TITLE, FIELD_BODY, FIELD_ANCHOR, FIELD_LANGUAGE };

    public KBAStreamDocument() {
        this.fields = new HashMap<String, String>();
    }

    public KBAStreamDocument(StreamItem si, String dirName) {
        initialize(si, dirName);
    }

    public KBAStreamDocument(StreamItem si, String dirName, List<String> mentioned_entities) {
        initialize(si, dirName, mentioned_entities);
    }

    private String getLanguage(ContentItem ci) {
        Language language = ci.getLanguage();

        if (language == null)
            return "";

        if (language.isSetCode())
            return language.getCode();

        else if (language.isSetName())
            return language.getName();

        return "";
    }

    protected void initialize(StreamItem si, String dirName) {
        fields = new HashMap<String, String>();
        //StringBuilder docId = new StringBuilder();
        //docId.append(dirName).append("-").append(si.getStream_id());
        fields.put(FIELD_DOCUMENT_ID, UUID.randomUUID().toString());
        fields.put(FIELD_STREAM_ID, si.getStream_id());
        fields.put(FIELD_DIR_NAME, dirName);
        fields.put(FIELD_SOURCE, si.getSource());
        fields.put(FIELD_SCHOST, si.getSchost());

        ContentItem body = si.getBody();
        fields.put(FIELD_BODY, getCleanVisible(body));
        fields.put(FIELD_LANGUAGE, getLanguage(body));

        Map<String, ContentItem> oc = si.getOther_content();
        if (oc != null) {
            fields.put(FIELD_TITLE, getCleanVisible(oc.get("title")));
            fields.put(FIELD_ANCHOR, getCleanVisible(oc.get("anchor")));
        } else {
            fields.put(FIELD_TITLE, "");
            fields.put(FIELD_ANCHOR, "");
        }
        
    }
    
    protected void initialize(StreamItem si, String dirName, List<String> mentioned_entities) {
        initialize(si, dirName);
        // TODO Find a better way if value is null
        this.mentionedEntities = mentioned_entities;
    }

    public void add(String key, String value) {
        fields.put(key, value);
    }

    public void setLanguage(String language) {
        fields.put(FIELD_LANGUAGE, language);
    }

    public String get(String key) {
        return fields.get(key);
    }

    public String getStreamId() {
        return fields.get(FIELD_STREAM_ID);
    }

    public void setSource(String value) {
        fields.put(FIELD_SOURCE, value);
    }

    public String getSource() {
        return fields.get(FIELD_SOURCE);
    }

    public String getStreamDate() {
        String date_hour = fields.get(FIELD_DIR_NAME);
        return date_hour.substring(0, (date_hour.lastIndexOf('-')));
    }

    public void setMentionedEntity(List<String> entity_list) {
        this.mentionedEntities = entity_list;
    }

    public List<String> getMentionedEntity() {
        return this.mentionedEntities;
    }

    public static String getCleanVisible(ContentItem ci) {
        if (ci == null)
            return "";
        if (ci.getClean_visible() == null || ci.getClean_visible().length() == 0)
            return "";

        if (ci.getClean_visible().length() > 0) {
            return ci.getClean_visible();
        } else {
            return "";
        }
    }

    public String getTextNormalized() {
        StringBuilder sb = new StringBuilder(fields.get(FIELD_TITLE).toLowerCase());
        sb.append(" ").append(fields.get(FIELD_BODY).toLowerCase());
        sb.append(" ").append(fields.get(FIELD_ANCHOR).toLowerCase());

        return sb.toString();
    }

    public String toString() {
        return "{" + "fields=" + fields + '}';
    }

    /**
     * Generate the ADM equivalent string
     * 
     * @return the ADM string
     */
    public String toAdmEquivalent() {
        ADMObject adm_object = new ADMObject();

        Iterator<Entry<String, String>> it = fields.entrySet().iterator();

        while (it.hasNext()) {
            Entry<String, String> e = it.next();
            String value = e.getValue();
            String key = e.getKey();
            if (value == null) {
                adm_object.put(key, "");
            } else if (key.equals(FIELD_BODY)) { // To deal with long string
                ADMArray admArray = new ADMOrderedArray(StringUtil.breakString(value, ASTERIX_STRING_LENGTH_LIMIT));
                adm_object.put(key, admArray);
            } else {
                adm_object.put(key, value);
            }
        }
        return adm_object.toString();
    }

}