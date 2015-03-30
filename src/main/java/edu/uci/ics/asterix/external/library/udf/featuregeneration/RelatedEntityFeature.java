package edu.uci.ics.asterix.external.library.udf.featuregeneration;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.external.dataset.adapter.KBARecord;
import edu.uci.ics.asterix.external.library.IFunctionHelper;
import edu.uci.ics.asterix.external.library.TopicEntity;
import edu.uci.ics.asterix.external.library.java.IJObject;
import edu.uci.ics.asterix.external.library.java.JTypeTag;
import edu.uci.ics.asterix.external.library.java.JObjects.JInt;
import edu.uci.ics.asterix.external.library.java.JObjects.JOrderedList;
import edu.uci.ics.asterix.external.library.java.JObjects.JRecord;
import edu.uci.ics.asterix.external.library.java.JObjects.JString;
import edu.uci.ics.asterix.external.library.java.JObjects.JUnorderedList;

public class RelatedEntityFeature extends AbstractFeatureGenerator {
    private Map<String, Set<String>> relatedEntityMap;

    private EntitySearcher searcher;

    public RelatedEntityFeature(EntitySearcher searcher, Map<String, Set<String>> relatedEntityMap) {
        super();
        this.searcher = searcher;
        this.relatedEntityMap = relatedEntityMap;
    }

    public RelatedEntityFeature(HashMap<String, String> props, EntitySearcher searcher) {
        super(props);
        this.searcher = searcher;
    }

    @Override
    public String[] getFeatureNames() {
        return EReltatedEntityFeature.getNames();
    }

    @Override
    public String[] getFeatureTypes() {
        return EReltatedEntityFeature.getTypes();
    }

    @Override
    public String getFeatureVector(KBARecord streamDoc, TopicEntity entity) {

        // use the same analyzer for doc text that we use for entities
        String title = (String) streamDoc.get(KBARecord.FIELD_TITLE);
        String body = (String) streamDoc.get(KBARecord.FIELD_BODY);
        String anchor = (String) streamDoc.get(KBARecord.FIELD_ANCHOR);

        Set<String> relatedEntities = entity.getRelatedEntities();
        int numTitle = searcher.numOfMentionedEntities(title, relatedEntities);
        int numBody = searcher.numOfMentionedEntities(body, relatedEntities);
        int numAnchor = searcher.numOfMentionedEntities(anchor, relatedEntities);

        StringBuilder sb = new StringBuilder();

        sb.append(relatedEntities.size() + "\t"); // Rel
        sb.append(numTitle + "\t"); // Rel(D_title, E)
        sb.append(numBody + "\t"); // Rel(D_body, E)
        sb.append(numAnchor); // Rel(D_anchor, E)

        return sb.toString();
    }

    @Override
    public JRecord getResultRecord(IFunctionHelper functionHelper, Map<String, Integer> fieldPositions) {
        this.functionHelper = functionHelper;
        
        // Get the input record from the feed
        JRecord inputRecord = (JRecord) functionHelper.getArgument(0);

        IJObject[] fields = inputRecord.getFields();

        String body = conctatenateBodyContent((JOrderedList) fields[fieldPositions.get(KBARecord.FIELD_BODY)]);

        JString jString = (JString) fields[fieldPositions.get(KBARecord.FIELD_TITLE)];
        String title = jString.getValue();

        jString = (JString) fields[fieldPositions.get(KBARecord.FIELD_ANCHOR)];
        String anchor = jString.getValue();

        int numTitle = 0;
        int numBody = 0;
        int numAnchor = 0;
        int numRelated = 0;

        JUnorderedList mentionUrlNames = (JUnorderedList) fields[fieldPositions.get(KBARecord.FIELD_MENTIONS)];
        if (mentionUrlNames != null && !mentionUrlNames.isEmpty()) {
            for (int i = 0; i < mentionUrlNames.size(); i++) {
                JString mention = (JString) mentionUrlNames.getElement(i);
                Set<String> relatedEntities = relatedEntityMap.get(mention.getValue());
                numTitle += searcher.numOfMentionedEntities(title, relatedEntities);
                numBody += searcher.numOfMentionedEntities(body, relatedEntities);
                numAnchor += searcher.numOfMentionedEntities(anchor, relatedEntities);
                numRelated += relatedEntities.size();
            }
        }
        
        // Generate results
        JRecord result = (JRecord) functionHelper.getResultObject();
        try {
            result.setField("doc_id", fields[fieldPositions.get(KBARecord.FIELD_DOCUMENT_ID)]);
            result.setField(EReltatedEntityFeature.RELATED.getName(), setValue(JTypeTag.INT, numRelated));
            result.setField(EReltatedEntityFeature.RELATEDTITLE.getName(), setValue(JTypeTag.INT, numTitle));
            result.setField(EReltatedEntityFeature.RELATEDBODY.getName(), setValue(JTypeTag.INT, numBody));         
            result.setField(EReltatedEntityFeature.RELATEDANCHOR.getName(), setValue(JTypeTag.INT, numAnchor));
        } catch (AsterixException e) {
            e.printStackTrace();
        }

        return result;
    }

}
