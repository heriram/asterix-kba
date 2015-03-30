package edu.uci.ics.asterix.external.library.utils;

import edu.uci.ics.asterix.external.dataset.adapter.KBARecord;
import edu.uci.ics.asterix.feeds.CentralFeedManager;

/**
 * @author heri
 */

public class KBAStreamingInitializer {

    private final static String STRING_TYPE = "string";
    private final static String ORDEREDLIST_STRING_TYPE = "[string]";
    private final static String UNORDEREDLIST_STRING_TYPE = "{{string}}";

    public final static String KBA_DATAVERSE = "feeds";
    public final static String KBA_INPUT_RECORD_TYPE = "InputRecordType";
    public final static String KBA_DOC_STREAM_TYPE = "StreamType";
    public final static String KBA_MAIN_DATASET = "StreamDocuments";
    public final static String KBA_SUBDOCS_DATASET = "StreamSubDocuments";
    public final static String KBA_SUBDOCS_TYPE = "ChildStreamType";

    public final static String KBA_PUSH_FEED = "kbalib#push_kba_stream_feed";

    public final static String KBA_CORPUS_PATH = "/Users/heri/git/corpus_test";

    public final static String KBA_DDL = "";

    public enum EFields {
        FIELD_DOCUMENT_ID(KBARecord.FIELD_DOCUMENT_ID, STRING_TYPE, false),
        FIELD_STREAM_ID(KBARecord.FIELD_STREAM_ID, STRING_TYPE, false),
        FIELD_TITLE(KBARecord.FIELD_TITLE, STRING_TYPE, false),
        FIELD_BODY(KBARecord.FIELD_BODY, ORDEREDLIST_STRING_TYPE, false),
        FIELD_SOURCE(KBARecord.FIELD_SOURCE, STRING_TYPE, false),
        FIELD_DIR_NAME(KBARecord.FIELD_DIR_NAME, STRING_TYPE, false),
        FIELD_ANCHOR(KBARecord.FIELD_ANCHOR, STRING_TYPE, false),
        FIELD_LANGUAGE(KBARecord.FIELD_LANGUAGE, STRING_TYPE, false),
        FIELD_SCHOST(KBARecord.FIELD_SCHOST, STRING_TYPE, false),
        FIELD_MENTIONS(KBARecord.FIELD_MENTIONS, UNORDEREDLIST_STRING_TYPE, false),
        FIELD_PARENT(KBARecord.FIELD_PARENT, STRING_TYPE, true);

        private final String fieldName;
        private final String fieldType;
        private final boolean isOptional;

        /**
         * @param text
         */
        private EFields(final String fieldName, final String fieldType, boolean isOptional) {
            this.fieldName = fieldName;
            this.fieldType = fieldType;
            this.isOptional = isOptional;
        }

        public String getType() {
            return fieldType;
        }

        public String getFieldSpecString() {
            return fieldName + ":" + fieldType + (isOptional ? "?" : "");
        }

        @Override
        public String toString() {
            return fieldName;
        }
    }

    private static String createFeedText(boolean prefilter) {
        StringBuilder builder = new StringBuilder();
        builder.append("create feed kbafeed using \"" + KBA_PUSH_FEED + "\"\n");
        builder.append("((\"type-name\"=\"" + KBA_INPUT_RECORD_TYPE + "\"), (\"batch-size\"=\"300\"),\n");
        builder.append("(\"pre-filter-mentions\"=\"" + (prefilter ? "yes" : "no") + "\"),\n"
                + "(\"path\"=\"127.0.0.1://" + KBA_CORPUS_PATH + "\"));\n");
        builder.append("connect feed kbafeed to dataset " + KBA_MAIN_DATASET + ";");

        return builder.toString();
    }

    private static String createSubDocumentText() {
        StringBuilder builder = new StringBuilder();
        // For the child record
        builder.append("\ndrop dataset " + KBA_SUBDOCS_DATASET + " if exists;\n");
        builder.append("drop type " + KBA_SUBDOCS_TYPE + " if exists;\n");
        builder.append("create type " + KBA_SUBDOCS_TYPE + " as open {\n");
        builder.append(EFields.FIELD_DOCUMENT_ID.getFieldSpecString() + ",\n");
        builder.append(EFields.FIELD_BODY.getFieldSpecString() + ",\n");
        builder.append(EFields.FIELD_PARENT.getFieldSpecString() + "\n");
        builder.append("}\n");
        builder.append("create dataset " + KBA_SUBDOCS_DATASET + "(" + KBA_SUBDOCS_TYPE + ") ");
        builder.append("primary key " + KBARecord.FIELD_DOCUMENT_ID + ";\n");

        return builder.toString();
    }

    private static String createMainDocumentText() {
        StringBuilder builder = new StringBuilder();

        builder.append("create dataverse " + KBA_DATAVERSE + " if not exists;" + "\n");
        builder.append("use dataverse " + KBA_DATAVERSE + ";" + "\n");
        builder.append("drop dataset " + KBA_MAIN_DATASET + " if exists;\n");
        builder.append("drop type " + KBA_INPUT_RECORD_TYPE + " if exists;\n");
        builder.append("drop type " + KBA_DOC_STREAM_TYPE + " if exists;\n");

        builder.append("create type " + KBA_INPUT_RECORD_TYPE + " as open {\n");

        int i = 0;
        for (EFields field : EFields.values()) {
            if (i > 0) {
                builder.append(",\n");
            }
            builder.append(field.getFieldSpecString());
            i++;
        }
        builder.append("\n}" + ";" + "\n");

        builder.append("create dataset " + KBA_MAIN_DATASET + " " + "(" + KBA_INPUT_RECORD_TYPE + ")" + " "
                + "primary key " + KBARecord.FIELD_DOCUMENT_ID + ";");

        return builder.toString();
    }

    public static void main(String[] args) throws Exception {
        StringBuilder sb = new StringBuilder();
        try {
            sb.append(createMainDocumentText());
            sb.append(createSubDocumentText());
            sb.append(createFeedText(true));

            //System.out.println(sb);
            CentralFeedManager.AQLExecutor.executeAQL(sb.toString());
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error: " + sb.toString());
            throw e;
        }
    }

}
