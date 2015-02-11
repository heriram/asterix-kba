package edu.uci.ics.asterix.external.library.utils;

import java.io.IOException;
import java.util.UUID;

import org.trec.kba.streamcorpus.StreamItem;

public class KBAStreamDocumentWriter extends KBAStreamDocument {
    public final static String FIELD_PARTITION_NUMBER = "doc_part_number";
    public final static String FIELD_PARENT = "parent";

    private BufferedStreamWriter writer;
    private int maxTupleSize = ASTERIX_STRING_LENGTH_LIMIT;
    private int bodySize;

    public KBAStreamDocumentWriter(StreamItem si, String dirName, BufferedStreamWriter writer, int maxFramSize) {
        super(si, dirName);
        this.writer = writer;
        maxTupleSize = maxFramSize > ASTERIX_STRING_LENGTH_LIMIT ? maxFramSize : ASTERIX_STRING_LENGTH_LIMIT;

        String text = fields.get(FIELD_BODY);

        if (text != null)
            this.bodySize = text.getBytes().length;
        else {
            bodySize = 0;
        }

    }

    private void writeDocumentPart(KBAStreamDocument docPart, String bodyText, int partNumber, String parentId)
            throws IOException {
        String[] fieldNamesToKeep = { FIELD_STREAM_ID, FIELD_DIR_NAME, FIELD_SOURCE, FIELD_SCHOST, FIELD_TITLE,
                FIELD_ANCHOR, FIELD_LANGUAGE };

        for (String key : fieldNamesToKeep) {
            docPart.add(key, fields.get(key));
        }

        // Setup child-specific fields
        String docId = UUID.randomUUID().toString();
        docPart.add(FIELD_DOCUMENT_ID, docId);
        docPart.add(FIELD_PARENT, parentId);
        docPart.add(FIELD_PARTITION_NUMBER, Integer.toString(partNumber));

        // Set body field
        docPart.add(FIELD_BODY, bodyText);

        // Flush this document part
        writer.writeStreamADMString(docPart.toAdmEquivalent() + '\n');

    }

    /* Write document parts as several smaller adm objects */
    private void writePartitioned() throws IOException {
        String body_text_parts[] = StringUtil.breakString(fields.get(FIELD_BODY), maxTupleSize);

        String parentId = fields.get(FIELD_DOCUMENT_ID);
        for (int i = 1; i < body_text_parts.length; i++) {
            KBAStreamDocument docPart = new KBAStreamDocument();
            docPart = new KBAStreamDocument();
            writeDocumentPart(docPart, body_text_parts[i], i, parentId);
        }
        // Set current document body part (the parent document)
        fields.put(FIELD_BODY, body_text_parts[0]);
        writer.writeStreamADMString(toAdmEquivalent() + '\n');

    }

    public void writeToOutput() throws IOException {
        if (bodySize < maxTupleSize)
            writer.writeStreamADMString(toAdmEquivalent() + '\n');

        else
            writePartitioned();
    }

}
