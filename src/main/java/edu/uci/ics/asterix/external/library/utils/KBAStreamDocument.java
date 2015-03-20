package edu.uci.ics.asterix.external.library.utils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.trec.kba.streamcorpus.ContentItem;
import org.trec.kba.streamcorpus.Language;
import org.trec.kba.streamcorpus.StreamItem;

import edu.uci.ics.asterix.external.library.utils.ADM.ADMArray;
import edu.uci.ics.asterix.external.library.utils.ADM.ADMObject;
import edu.uci.ics.asterix.external.library.utils.ADM.ADMOrderedArray;

public class KBAStreamDocument {

    protected final static int ASTERIX_STRING_LENGTH_LIMIT = 32745; // See UTF8StringWriter.java

    protected Set<String> mentionedEntities = null;

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
     */
    protected String[] fieldNames = { FIELD_DOCUMENT_ID, FIELD_STREAM_ID, FIELD_TITLE, FIELD_BODY, FIELD_SOURCE,
            FIELD_DIR_NAME, FIELD_ANCHOR, FIELD_LANGUAGE, FIELD_SCHOST };

    public KBAStreamDocument() {
        this.fields = new HashMap<String, String>();
    }

    public KBAStreamDocument(StreamItem si, String dirName) {
        initialize(si, dirName);
    }

    public KBAStreamDocument(StreamItem si, String dirName, Set<String> mentioned_entities) {
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

    public int getNumBytes() {
        int size = 0;
        for (String fieldName : fieldNames) {
            String s = fields.get(fieldName);
            size += s.getBytes().length;
        }

        return size;
    }
    
    public String[] getBodyText(int maxLen) {
        return StringUtil.breakString(fields.get(FIELD_BODY), maxLen);
    }
    
    protected void initialize(StreamItem si, String dirName, boolean newDocId) {
        fields = new HashMap<String, String>();
        String docid = null;
        if (newDocId) {
            // Create an unique doc id
            UUID uuid = UUID.randomUUID();
            docid = uuid.toString();
            
        } else {
            docid = si.getDoc_id();
        }
        
        fields.put(FIELD_DOCUMENT_ID, docid);
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
        
        si.clear();
    }

    protected void initialize(StreamItem si, String dirName) {
        initialize(si, dirName, true);
    }

    protected void initialize(StreamItem si, String dirName, Set<String> mentioned_entities) {
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

    public void setMentionedEntity(Set<String> entity_list) {
        this.mentionedEntities = entity_list;
    }

    public Set<String> getMentionedEntity() {
        return this.mentionedEntities;
    }

    public String getCleanVisible(ContentItem ci) {
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