package edu.uci.ics.asterix.external.library.utils;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.trec.kba.streamcorpus.ContentItem;
import org.trec.kba.streamcorpus.Language;
import org.trec.kba.streamcorpus.StreamItem;

import edu.uci.ics.asterix.external.library.utils.ADM.ADMArray;
import edu.uci.ics.asterix.external.library.utils.ADM.ADMObject;
import edu.uci.ics.asterix.external.library.utils.ADM.ADMOrderedArray;

public class KBAAdmStreamDocument {
    private static final Logger LOGGER = Logger.getLogger(KBAAdmStreamDocument.class.getName());
    
    protected final static int ASTERIX_STRING_LENGTH_LIMIT = 32745; // See UTF8StringWriter.java

    private BufferedStreamWriter writer;

    private int maxTupleSize = ASTERIX_STRING_LENGTH_LIMIT;

    private int bodySize;
    
    private String fullBodyString;

    List<String> mentionedEntities = null;
    
    /*
    The DDL for this object: 
    
    create type StreamType as open { 
        * doc_id: string,
        * stream_id: string,
        * title_cleansed: string,
        * body_cleansed: [string],
        * source: string,
        * dir_name: string,
        * anchor_cleansed: string,
        * language: string,
        * schost: string,
        doc_part_number:string?,
        parent:string?
    }
    create dataset StreamDocuments(StreamType)
    primary key doc_id;
       
     */

    public final static String FIELD_DOCUMENT_ID = "doc_id";
    public final static String FIELD_STREAM_ID = "stream_id";
    public final static String FIELD_DIR_NAME = "dir_name";
    public final static String FIELD_SOURCE = "source";
    public final static String FIELD_SCHOST = "schost";
    public final static String FIELD_TITLE = "title_cleansed";
    public final static String FIELD_BODY = "body_cleansed";
    public final static String FIELD_ANCHOR = "anchor_cleansed";
    public final static String FIELD_LANGUAGE = "language";

    public final static String FIELD_PARTITION_NUMBER = "doc_part_number";
    public final static String FIELD_PARENT = "parent";

    private ADMObject adm;

    /**
     * Field names
     */
    protected static String[] fieldNames = { FIELD_BODY, FIELD_DOCUMENT_ID, FIELD_STREAM_ID, FIELD_DIR_NAME,
            FIELD_SOURCE, FIELD_SCHOST, FIELD_TITLE, FIELD_ANCHOR, FIELD_LANGUAGE };

    public KBAAdmStreamDocument() {
        this.adm = new ADMObject();
    }

    public KBAAdmStreamDocument(StreamItem si, String dirName) {
        initialize(si, dirName);
    }

    public KBAAdmStreamDocument(StreamItem si, String dirName, List<String> mentioned_entities) {
        initialize(si, dirName, mentioned_entities);
    }

    public KBAAdmStreamDocument(StreamItem si, String dirName, BufferedStreamWriter writer, int maxFramSize) {
        initialize(si, dirName);
        
        this.writer = writer;
        maxTupleSize = maxFramSize > ASTERIX_STRING_LENGTH_LIMIT ? maxFramSize : ASTERIX_STRING_LENGTH_LIMIT;

        if (this.fullBodyString != null) {
            this.bodySize = StringUtil.sizeOfString(this.fullBodyString);
        } else {
            bodySize = 0;
        }
    }

    protected static String getCleanVisible(ContentItem ci) {
        if (ci == null)
            return "";
        if (ci.getClean_visible() == null || ci.getClean_visible().length() == 0)
            return "";

        if (ci.getClean_visible().length() > 0) {
            // Cleaned version... remember to clean the Entity list too...
            return StringUtil.removeSpecialChars(ci.getClean_visible());
        } else {
            return "";
        }
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
        this.adm = new ADMObject();

        // Create an unique doc id
        UUID uuid = UUID.randomUUID();
        String docid = uuid.toString();
        
        // doc_id: string
        adm.put(FIELD_DOCUMENT_ID, docid);

        // stream_id: string
        adm.put(FIELD_STREAM_ID, si.getStream_id());

        // dir_name: string
        adm.put(FIELD_DIR_NAME, dirName);

        // source: string
        String source = si.getSource();
        if (source==null) source = "";
        adm.put(FIELD_SOURCE, source);

        // schost: string
        String schost = si.getSchost();
        if (schost==null) schost = "";
        adm.put(FIELD_SCHOST, schost);

        // body_cleansed: [string]
        ContentItem body = si.getBody();
        this.fullBodyString = getCleanVisible(body);
        // Makes sure that the document body can be stored in Asterix
        ADMArray admArray = new ADMOrderedArray(StringUtil.breakString(fullBodyString, ASTERIX_STRING_LENGTH_LIMIT));
        adm.put(FIELD_BODY, admArray);

        // language: string
        adm.put(FIELD_LANGUAGE, getLanguage(body));

        Map<String, ContentItem> oc = si.getOther_content();
        if (oc != null) {
            // title_cleansed: string
            adm.put(FIELD_TITLE, getCleanVisible(oc.get("title")));

            // anchor_cleansed: string
            adm.put(FIELD_ANCHOR, getCleanVisible(oc.get("anchor")));
        } else {
            //title_cleansed: string
            adm.put(FIELD_TITLE, "");

            // anchor_cleansed: string
            adm.put(FIELD_ANCHOR, "");
        }
        
        si.clear();

    }
    
    private boolean controlFields() {
        boolean pass=true;
        
        Iterator<String> keys = adm.keys();
        
        while (keys.hasNext()) {
            String key = keys.next();
            if (!key.equals(FIELD_BODY) && adm.getString(key)==null) {
                System.err.println("Field: " + key + " does not have String value.");
                return false;
            }
        }
        
        return pass;
    }
    
    public void add(String key, Object value) {
        this.adm.put(key, value);
    }

    private void setBodyField(String newBodyString) {
        ADMArray admArray = new ADMOrderedArray(StringUtil.breakString(newBodyString, ASTERIX_STRING_LENGTH_LIMIT));
        adm.put(FIELD_BODY, admArray);
    }
    
    
    protected void initialize(StreamItem si, String dirName, List<String> mentioned_entities) {
        initialize(si, dirName);
        // TODO Find a better way if value is null
        this.mentionedEntities = mentioned_entities;
    }

    public String toString() {
        return adm.toString();
    }

    private void writeDocumentPart(KBAAdmStreamDocument docPart, String bodyText, int partNumber, String parentId)
            throws IOException {
        String[] fieldNamesToKeep = { FIELD_STREAM_ID, FIELD_DIR_NAME, FIELD_SOURCE, FIELD_SCHOST, FIELD_TITLE,
                FIELD_ANCHOR, FIELD_LANGUAGE };

        for (String key : fieldNamesToKeep) {
            docPart.add(key, adm.getString(key));
        }

        // Setup child-specific fields
        String docId = UUID.randomUUID().toString();
        docPart.add(FIELD_DOCUMENT_ID, docId);
        docPart.add(FIELD_PARENT, parentId);
        docPart.add(FIELD_PARTITION_NUMBER, Integer.toString(partNumber));

        // Set body field
        docPart.setBodyField(bodyText);

        
        // Flush this document part
        String admString = docPart.toString();
        writer.writeStreamADMString(admString);

    }

    /* Write document parts as several smaller adm objects */
    private void writePartitioned() throws IOException {
        String bodyTextParts[] = StringUtil.breakString(this.fullBodyString, maxTupleSize);

        String parentId = (String) adm.get(FIELD_DOCUMENT_ID);
        for (int i = 1; i < bodyTextParts.length; i++) {
            KBAAdmStreamDocument docPart = new KBAAdmStreamDocument();
            writeDocumentPart(docPart, bodyTextParts[i], i, parentId);
        }
        
        // Set current document body part (the parent document)
        setBodyField(bodyTextParts[0]);
        String admString = this.toString() + '\n';
        writer.writeStreamADMString(admString);

    }

    public void writeToOutputSream() throws IOException {       
        if (bodySize < maxTupleSize) {
            String admString = this.toString();
            writer.writeStreamADMString(admString);
        }
        else
            writePartitioned();
    }
}
