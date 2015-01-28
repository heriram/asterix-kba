package edu.uci.ics.asterix.external.library.adaptor;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.trec.kba.streamcorpus.ContentItem;
import org.trec.kba.streamcorpus.StreamItem;

public class KBAStreamDoc {

    List<String> mentionedEntities = null;

    protected HashMap<String, String> fields;

    public final static String FIELD_DOC_ID = "doc_id";
    public final static String FIELD_STREAM_ID = "stream_id";
    public final static String FIELD_DIR_NAME = "dir_name";
    public final static String FIELD_SOURCE = "source";
    public final static String FIELD_SCHOST = "schost";
    public final static String FIELD_EPOCH_TICKS = "epoch_ticks";
    public final static String FIELD_ZULU_TIMESTAMP = "zulu_timestamp";
    public final static String FIELD_TITLE = "title_cleansed";
    public final static String FIELD_BODY = "body_cleansed";
    public final static String FIELD_ANCHOR = "anchor_cleansed";
    public final static String FIELD_LANGUAGE = "language";

    /**
     * Field names
     */
    protected static String[] fieldNames = { FIELD_DOC_ID, FIELD_STREAM_ID, FIELD_DIR_NAME, FIELD_SOURCE,
            FIELD_SCHOST, FIELD_EPOCH_TICKS, FIELD_ZULU_TIMESTAMP, FIELD_TITLE, FIELD_BODY, FIELD_ANCHOR,
            FIELD_LANGUAGE };

    public KBAStreamDoc() {
        fields = new HashMap<String, String>();
    }

    public KBAStreamDoc(StreamItem si, String dirName) {
        reset(si, dirName, null);
    }

    public KBAStreamDoc(StreamItem si, String dirName, List<String> mentioned_entities) {
        reset(si, dirName, mentioned_entities);
    }

    public void reset(StreamItem si, String dirName, List<String> mentioned_entities) {
        fields = new HashMap<String, String>();
        fields.put(FIELD_STREAM_ID, si.getStream_id());
        fields.put(FIELD_DOC_ID, si.getDoc_id());
        fields.put(FIELD_DIR_NAME, dirName);
        fields.put(FIELD_SOURCE, si.getSource());
        fields.put(FIELD_SCHOST, si.getSchost());
        fields.put(FIELD_ZULU_TIMESTAMP, si.getStream_time().getZulu_timestamp());
        fields.put(FIELD_EPOCH_TICKS, Double.toString(si.getStream_time().getEpoch_ticks()));
        fields.put(FIELD_BODY, getCleanVisible(si.getBody()));
        fields.put(FIELD_TITLE, getCleanVisible(si.getOther_content().get("title")));
        fields.put(FIELD_ANCHOR, getCleanVisible(si.getOther_content().get("anchor")));

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

    public void setStreamId(String value) {
        fields.put(FIELD_STREAM_ID, value);
    }

    public void setBody(String value) {
        fields.put(FIELD_BODY, value);
    }

    public String getBody() {
        return fields.get(FIELD_BODY);
    }

    public String getTitle() {
        return fields.get(FIELD_TITLE);
    }

    public void setTitle(String value) {
        fields.put(FIELD_TITLE, value);
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

    public String getDirName() {
        return fields.get(FIELD_DIR_NAME);
    }

    public void setDirName(String dirname) {
        fields.put(FIELD_DIR_NAME, dirname);
    }

    public void setMentionedEntity(List<String> entity_list) {
        this.mentionedEntities = entity_list;
    }

    public List<String> setMentionedEntity() {
        return this.mentionedEntities;
    }

    public static String getCleanVisible(ContentItem ci) {
        if (ci == null)
            return null;
        if (ci.getClean_visible() == null || ci.getClean_visible().length() == 0)
            return null;

        if (ci.getClean_visible().length() > 0) {
            return ci.getClean_visible();
        } else {
            return null;
        }
    }

    public static String strip(String s) {
        final String specialChars2 = "[" + Pattern.quote("`~!#$%^*()_+[\\];',./{}|:\"<>?") + "]+";

        s = s.replaceAll("(\\s)+", " ");
        s = s.replaceAll("'s", "");
        s = s.replaceAll(specialChars2, " ");
        return s;
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
        StringBuilder sb = new StringBuilder("{");
        Iterator<Entry<String, String>> it = fields.entrySet().iterator();
        int i = 0;
        int size = fields.entrySet().size();
        while (it.hasNext()) {
            Entry<String, String> e = it.next();
            sb.append('\"').append(e.getKey()).append("\":\"");
            String value = e.getValue();
            if (value == null)
                value = "";
            sb.append(value).append("\"");
            i++;
            if (i < size)
                sb.append(",\n");
        }

        sb.append("}");
        return sb.toString();
    }

}