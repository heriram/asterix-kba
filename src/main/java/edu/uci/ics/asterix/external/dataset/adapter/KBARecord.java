package edu.uci.ics.asterix.external.dataset.adapter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;

import org.trec.kba.streamcorpus.ContentItem;
import org.trec.kba.streamcorpus.Language;
import org.trec.kba.streamcorpus.StreamItem;

import edu.uci.ics.asterix.external.library.PhraseFinder;
import edu.uci.ics.asterix.external.library.udf.featuregeneration.EntitySearcher;
import edu.uci.ics.asterix.external.library.utils.KBAStreamDocument;
import edu.uci.ics.asterix.external.library.utils.LanguageDetector;
import edu.uci.ics.asterix.external.library.utils.StringPool;
import edu.uci.ics.asterix.external.library.utils.StringUtil;
import edu.uci.ics.asterix.om.types.ARecordType;

public class KBARecord extends KBAStreamDocument {

    @SuppressWarnings("unused")
    private static final long serialVersionUID = 1L;
    
    private final StringPool stringInterner = StringPool.INSTANCE; 

    public Map<String, ContentItem> oc;

    private HashMap<String, Object> fields;

    public final static String FIELD_MENTIONS = "mentions";
    public final static String FIELD_PARENT = "parent_id";
    public final static String FIELD_PART = "part_number";

    private String dirName;
    private String docId;
    private String bodyText;
    private String title;

    private LanguageDetector languageDetector;

    public final static KBARecord INSTANCE = new KBARecord();

    public KBARecord() {
        super();
    }

    public KBARecord(LanguageDetector languageDetector) {
        super();
        this.languageDetector = languageDetector;
    }

    public KBARecord(StreamItem streamItem, String dirName) {
        this.initialize(streamItem, dirName);
    }

    public boolean isEmpty() {
        return (bodyText.isEmpty() && title.isEmpty());
    }

    @Override
    public void initialize(StreamItem streamItem, String dirName) {
        this.dirName = dirName;
        this.docId = UUID.randomUUID().toString();

        this.oc = streamItem.getOther_content();

        fields = new HashMap<String, Object>();

        fields.put(FIELD_DOCUMENT_ID, this.docId);
        fields.put(FIELD_STREAM_ID, streamItem.getStream_id());

        fields.put(FIELD_DIR_NAME, dirName);

        fields.put(FIELD_SOURCE, streamItem.getSource());

        fields.put(FIELD_SCHOST, streamItem.getSchost());

        ContentItem body = streamItem.getBody();
        this.bodyText = getCleanVisible(body);
        fields.put(FIELD_BODY, bodyText);
        fields.put(FIELD_LANGUAGE, getLanguage(body));

        fields.put(FIELD_TITLE, getTitle());

        fields.put(FIELD_ANCHOR, getAnchor());

        streamItem.clear();

    }

    public void setFieldValues(ARecordType recordType, StreamItem streamItem, String dirName) {
        this.fieldNames = recordType.getFieldNames();
        this.dirName = dirName;
        this.docId = UUID.randomUUID().toString();

        this.oc = streamItem.getOther_content();

        fields = new HashMap<String, Object>();
        ContentItem body = streamItem.getBody();

        for (String fieldName : fieldNames) {
            if (fieldName.equals(FIELD_DOCUMENT_ID))
                fields.put(FIELD_DOCUMENT_ID, docId);
            else if (fieldName.equals(FIELD_STREAM_ID))
                fields.put(FIELD_STREAM_ID, streamItem.getStream_id());
            else if (fieldName.equals(FIELD_DIR_NAME))
                fields.put(FIELD_DIR_NAME, dirName);
            else if (fieldName.equals(FIELD_SOURCE))
                fields.put(FIELD_SOURCE, streamItem.getSource());
            else if (fieldName.equals(FIELD_SCHOST))
                fields.put(FIELD_SCHOST, streamItem.getSchost());
            else if (fieldName.equals(FIELD_BODY)) {
                this.bodyText = getCleanVisible(body);
                fields.put(FIELD_BODY, bodyText);
            } else if (fieldName.equals(FIELD_TITLE))
                fields.put(FIELD_TITLE, getTitle());
            else if (fieldName.equals(FIELD_ANCHOR))
                fields.put(FIELD_ANCHOR, getAnchor());
            else if (fieldName.equals(FIELD_MENTIONS)) {
                this.mentionedEntities = new HashSet<String>();
                fields.put(FIELD_MENTIONS, mentionedEntities);
            }
        }

        String language = getLanguage(body);
        if (language == null) {
            if (bodyText!=null && !bodyText.isEmpty())
                language = languageDetector.detect(bodyText);

            if (language == null || language.isEmpty()) {
                language = StringUtil.EMPTY_STRING;
            }
        }
        fields.put(FIELD_LANGUAGE, language);

        streamItem.clear();
    }

    public String[] getFieldNames() {
        return fieldNames;
    }
    
    private String getLanguage(ContentItem ci) {
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

    public String getDirName() {
        return this.dirName;
    }

    //@Override
    public String getDoc_id() {
        return this.docId;
    }

    @Override
    public String getCleanVisible(ContentItem ci) {
        if (ci == null)
            return StringUtil.EMPTY_STRING;
        if (ci.getClean_visible() == null || ci.getClean_visible().length() == 0)
            return StringUtil.EMPTY_STRING;

        if (ci.getClean_visible().length() > 0) {
            return stringInterner.manualIntern(StringUtil.cleanText(ci.getClean_visible()));
        } else {
            return StringUtil.EMPTY_STRING;
        }
    }

    public String getTitle() {
        if (oc != null)
            this.title = getCleanVisible(oc.get("title"));
        else
            title = StringUtil.EMPTY_STRING;

        return title;
    }

    public String getAnchor() {
        if (oc != null)
            return getCleanVisible(oc.get("anchor"));
        else
            return StringUtil.EMPTY_STRING;
    }

    public String getBodyText() {
        return this.bodyText;
    }

    public String getContent() {
        return (getTitle() + " " + getBodyText());
    }

    public Map<String, Object> getFields() {
        return this.fields;
    }

    public void clear() {
        this.mentionedEntities.clear();
        fields.clear();
    }

    public boolean containMention(PhraseFinder mentionSearcher) {
        String content = getContent();
        if (content.trim().isEmpty())
            return false;
        return mentionSearcher.containMention(mentionedEntities, content);
    }

    public boolean containMention(PhraseFinder mentionSearcher, Map<String, String> urlMap) {
        String content = getContent();
        if (content.trim().isEmpty())
            return false;
        return mentionSearcher.containMention(urlMap, mentionedEntities, content);
    }
    
    public boolean containMention(EntitySearcher mentionSearcher) {
        String content = getContent();
        if (content.trim().isEmpty())
            return false;
        return mentionSearcher.containMention(content, mentionedEntities);
    }

    public boolean containMention(EntitySearcher mentionSearcher, Map<String, String> urlMap) {
        String content = getContent();
        if (content.trim().isEmpty())
            return false;
        return mentionSearcher.containMention(content, urlMap, mentionedEntities);
    }
}
