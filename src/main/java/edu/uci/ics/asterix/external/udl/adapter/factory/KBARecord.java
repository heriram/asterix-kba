package edu.uci.ics.asterix.external.udl.adapter.factory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.lucene.analysis.Analyzer;
import org.trec.kba.streamcorpus.ContentItem;
import org.trec.kba.streamcorpus.Language;
import org.trec.kba.streamcorpus.StreamItem;

import edu.uci.ics.asterix.external.library.PhraseFinder;
import edu.uci.ics.asterix.external.library.utils.KBAStreamDocument;
import edu.uci.ics.asterix.external.library.utils.StringUtil;
import edu.uci.ics.asterix.external.library.utils.TextAnalysis;
import edu.uci.ics.asterix.om.types.ARecordType;

public class KBARecord extends KBAStreamDocument {

    @SuppressWarnings("unused")
    private static final long serialVersionUID = 1L;

    public Map<String, ContentItem> oc;

    private HashMap<String, Object> fields;

    public final static String FIELD_MENTIONS = "mentions";

    private String dirName;
    private String docId;
    private String bodyText;
    private String title;

    public static final Analyzer ANALYZER = TextAnalysis.getAnalyzer();

    public KBARecord() {
        super();
    }

    public KBARecord(StreamItem streamItem, String dirName) {
        this.initialize(streamItem, dirName);
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
            } else if (fieldName.equals(FIELD_LANGUAGE))
                fields.put(FIELD_LANGUAGE, getLanguage(body));
            else if (fieldName.equals(FIELD_TITLE))
                fields.put(FIELD_TITLE, getTitle());
            else if (fieldName.equals(FIELD_ANCHOR))
                fields.put(FIELD_ANCHOR, getAnchor());
            else if (fieldName.equals(FIELD_MENTIONS)) {
                this.mentionedEntities = new HashSet<String>();
                fields.put(FIELD_MENTIONS, mentionedEntities);
            }
        }

        streamItem.clear();
    }

    public String[] getFieldNames() {
        return fieldNames;
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
            return StringUtil.removeSpecialChars(ci.getClean_visible());
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

    public boolean containMention(Set<String[]> nameVariants) throws Exception {
        String content = getContent();

        if (content.trim().isEmpty())
            return false;

        Map<String, Set<Integer>> analyzed_text = new HashMap<String, Set<Integer>>();
        TextAnalysis.analyze(ANALYZER, content, analyzed_text);

        // Find an entity mentioned in the text
        Iterator<String[]> nameVariantsIterator = nameVariants.iterator();
        while (nameVariantsIterator.hasNext()) {
            String name_terms[] = nameVariantsIterator.next();
            if (PhraseFinder.find(analyzed_text, name_terms) == true) {
                return true;
            }
        }

        return false;
    }

    public Map<String, Object> getFields() {
        return this.fields;
    }

    public void clear() {
        this.mentionedEntities.clear();
        this.fields.clear();
    }

    public boolean containMention(String[][] nameVariants) throws Exception {
        return containMention(nameVariants, false);
    }

    public boolean containMention(String[][] nameVariants, boolean findAll) throws Exception {
        String content = getContent();
        mentionedEntities.clear();

        if (content.trim().isEmpty())
            return false;

        Map<String, Set<Integer>> analyzed_text = new HashMap<String, Set<Integer>>();
        TextAnalysis.analyze(ANALYZER, content, analyzed_text);

        for (int i = 0; i < nameVariants.length; i++) {
            if (PhraseFinder.find(analyzed_text, nameVariants[i]) == true) {
                if (findAll)
                    this.mentionedEntities.add(StringUtil.concatenate(nameVariants[i], ' '));
                else
                    return true;
            }
        }

        if (findAll)
            return (!mentionedEntities.isEmpty());
        else
            return false;
    }
}
