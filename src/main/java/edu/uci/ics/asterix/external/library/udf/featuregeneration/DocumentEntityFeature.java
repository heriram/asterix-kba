package edu.uci.ics.asterix.external.library.udf.featuregeneration;

import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.external.library.IFunctionHelper;
import edu.uci.ics.asterix.external.library.TopicEntity;
import edu.uci.ics.asterix.external.library.java.IJObject;
import edu.uci.ics.asterix.external.library.java.JObjects.JDouble;
import edu.uci.ics.asterix.external.library.java.JObjects.JInt;
import edu.uci.ics.asterix.external.library.java.JObjects.JOrderedList;
import edu.uci.ics.asterix.external.library.java.JObjects.JRecord;
import edu.uci.ics.asterix.external.library.java.JObjects.JString;
import edu.uci.ics.asterix.external.library.utils.Util;
import edu.uci.ics.asterix.external.udl.adapter.factory.KBARecord;

public class DocumentEntityFeature extends AbstractFeatureGenerator {
    private EntitySearcher searcher;

    public DocumentEntityFeature(EntitySearcher searcher) {
        super();
        this.searcher = searcher;
    }

    public DocumentEntityFeature(HashMap<String, String> props, EntitySearcher searcher) {
        super(props);
        this.searcher = searcher;
    }

    @Override
    public String[] getFeatureNames() {
        return EDocumentEntityFeature.getNames();
    }

    @Override
    public String[] getFeatureTypes() {
        return EDocumentEntityFeature.getTypes();
    }

    @Override
    public String getFeatureVector(KBARecord streamDoc, TopicEntity entity) {
        // field lengths
        String title = streamDoc.get(KBARecord.FIELD_TITLE);
        int titleMentionPositions[] = searcher.search(title);

        String body = streamDoc.get(KBARecord.FIELD_BODY);
        int bodyMentionPositions[] = searcher.search(body);

        String anchor = streamDoc.get(KBARecord.FIELD_ANCHOR);
        int anchorMentionPositions[] = searcher.search(anchor);
        int bodyLength = tokenizer.tokenize(body).length;

        int fpos = 0;
        int lpos = 0;

        if (bodyMentionPositions == null && bodyLength > 0) {
            int maxMin[] = Util.findMaxAndMin(bodyMentionPositions);
            fpos = maxMin[0];
            lpos = maxMin[1];
        }

        // spread is 0 if there is only a single entity mention
        int spread = fpos > 0 ? lpos - fpos : 0;
        double fposNorm = bodyLength > 0 ? (double) fpos / bodyLength : 0.0;
        double lposNorm = bodyLength > 0 ? (double) lpos / bodyLength : 0.0;
        double spreadNorm = bodyLength > 0 ? (double) spread / bodyLength : 0.0;

        StringBuilder sb = new StringBuilder();
        sb.append(titleMentionPositions.length + "\t"); // N(D_title, E)
        sb.append(bodyMentionPositions.length + "\t"); // N(D_body, E)
        sb.append(anchorMentionPositions.length + "\t"); // N(D_anchor, E)
        sb.append(fpos + "\t"); // FPOS(D, E)
        sb.append(lpos + "\t"); // LPOS(D, E)
        sb.append(spread + "\t"); // SPR(D, E)
        sb.append(fposNorm + "\t"); // FPOS_norm(D, E)
        sb.append(lposNorm + "\t"); // LPOS_norm(D, E)
        sb.append(spreadNorm); // SPR_norm(D, E)

        return sb.toString();
    }

    @Override
    public JRecord getResultRecord(IFunctionHelper functionHelper, Map<String, Integer> fieldPositions) {
        // Get the input record from the feed
        JRecord inputRecord = (JRecord) functionHelper.getArgument(0);

        IJObject[] fields = inputRecord.getFields();

        String body = conctatenateBodyContent((JOrderedList) fields[fieldPositions.get(KBARecord.FIELD_BODY)]);
        int bodyLength = tokenizer.tokenize(body).length;
        int nBody = 0;
        int bodyMentionPositions[] = searcher.search(body);
        if (bodyMentionPositions != null)
            nBody = bodyMentionPositions.length;

        JString jString = (JString) fields[fieldPositions.get(KBARecord.FIELD_TITLE)];
        String title = jString.getValue();
        int nTitle = 0;
        
        if (title != null && !title.isEmpty()) {
            int titleMentionPositions[] = searcher.search(title);
            if (titleMentionPositions != null)
                nTitle = titleMentionPositions.length;
        }

        jString = (JString) fields[fieldPositions.get(KBARecord.FIELD_ANCHOR)];
        String anchor = jString.getValue();
        int nAnchor = 0;
        int anchorMentionPositions[] = searcher.search(anchor);
        if (anchorMentionPositions != null)
            nAnchor = anchorMentionPositions.length;

        int fpos = 0;
        int lpos = 0;

        if (bodyMentionPositions != null && bodyLength > 0) {
            int maxMin[] = Util.findMaxAndMin(bodyMentionPositions);
            fpos = maxMin[0];
            lpos = maxMin[1];
        }

        // spread is 0 if there is only a single entity mention
        int spread = fpos > 0 ? lpos - fpos : 0;
        double fposNorm = bodyLength > 0 ? (double) fpos / bodyLength : 0.0;
        double lposNorm = bodyLength > 0 ? (double) lpos / bodyLength : 0.0;
        double spreadNorm = bodyLength > 0 ? (double) spread / bodyLength : 0.0;

        // Generate results
        JRecord result = (JRecord) functionHelper.getResultObject();
        try {
            result.setField("doc_id", fields[fieldPositions.get(KBARecord.FIELD_DOCUMENT_ID)]);
            result.setField(EDocumentEntityFeature.MENTIONSTITLE.getName(), new JInt(nTitle)); // N(D_title, E)
            result.setField(EDocumentEntityFeature.MENTIONSBODY.getName(), new JInt(nBody)); // N(D_body, E)
            result.setField(EDocumentEntityFeature.MENTIONSANCHOR.getName(), new JInt(nAnchor)); // N(D_anchor, E)
            result.setField(EDocumentEntityFeature.FIRSTPOS.getName(), new JInt(fpos)); // FPOS(D, E)
            result.setField(EDocumentEntityFeature.LASTPOS.getName(), new JInt(lpos)); // LPOS(D, E)
            result.setField(EDocumentEntityFeature.SPREAD.getName(), new JInt(spread)); // SPR(D, E)
            result.setField(EDocumentEntityFeature.FIRSTPOSNORM.getName(), new JDouble(fposNorm)); // FPOS_norm(D, E)
            result.setField(EDocumentEntityFeature.LASTPOSNORM.getName(), new JDouble(lposNorm)); // LPOS_norm(D, E)
            result.setField(EDocumentEntityFeature.SPREADNORM.getName(), new JDouble(spreadNorm)); // SPR_norm(D, E)

        } catch (AsterixException e) {
            e.printStackTrace();
        }

        return result;

    }

}
