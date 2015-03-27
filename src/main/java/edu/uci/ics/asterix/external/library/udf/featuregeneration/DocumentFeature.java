package edu.uci.ics.asterix.external.library.udf.featuregeneration;

import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.external.dataset.adapter.KBARecord;
import edu.uci.ics.asterix.external.library.IFunctionHelper;
import edu.uci.ics.asterix.external.library.TopicEntity;
import edu.uci.ics.asterix.external.library.java.IJObject;
import edu.uci.ics.asterix.external.library.java.JTypeTag;
import edu.uci.ics.asterix.external.library.java.JObjects.JDouble;
import edu.uci.ics.asterix.external.library.java.JObjects.JInt;
import edu.uci.ics.asterix.external.library.java.JObjects.JOrderedList;
import edu.uci.ics.asterix.external.library.java.JObjects.JRecord;
import edu.uci.ics.asterix.external.library.java.JObjects.JString;
import edu.uci.ics.asterix.om.types.BuiltinType;

/**
 * Document features
 * 
 * @author Heri Ramampiaro <heri@idi.ntnu.no>
 * @author Krisztian Balog <krisztian.balog@idi.ntnu.no>
 */
public class DocumentFeature extends AbstractFeatureGenerator {

    public DocumentFeature() {
        super();
    }

    public DocumentFeature(HashMap<String, String> props) {
        super(props);
    }

    @Override
    public String[] getFeatureNames() {
        return EDocumentFeature.getNames();
    }

    @Override
    public String[] getFeatureTypes() {
        return EDocumentFeature.getTypes();
    }

    @Override
    public String getFeatureVector(KBARecord doc, TopicEntity entity) {

        // field lengths
        String title = doc.get(KBARecord.FIELD_TITLE);
        int titleLength = tokenizer.tokenize(title).length;
        String body = doc.get(KBARecord.FIELD_BODY);
        int bodyLength = tokenizer.tokenize(body).length;
        int anchorLength = tokenizer.tokenize(doc.get(KBARecord.FIELD_ANCHOR)).length;

        // source
        String src = "?";
        int source = ESource.getValueOfName(doc.get(KBARecord.FIELD_SOURCE));
        if (source!=-1)
            src =  source + "";

        // language detection
        String language = doc.get(KBARecord.FIELD_LANGUAGE);
        String lang = "0";

        if (language != null && language.equalsIgnoreCase("EN"))
            lang = "1";

        StringBuilder sb = new StringBuilder();
        sb.append(titleLength + "\t"); // LEN(D_title)
        sb.append(bodyLength + "\t"); // LEN(D_body)
        sb.append(anchorLength + "\t"); // LEN(D_anchor)
        sb.append(src + "\t"); // SRC(D)
        sb.append(lang); // LANG(D)

        return sb.toString();

    }

    public JRecord getResultRecord(IFunctionHelper functionHelper, Map<String, Integer> fieldPositions) {
        this.functionHelper = functionHelper;
        
        // Get the input record from the feed
        JRecord inputRecord = (JRecord) functionHelper.getArgument(0);

        IJObject[] fields = inputRecord.getFields();

        String body = conctatenateBodyContent((JOrderedList) fields[fieldPositions.get(KBARecord.FIELD_BODY)]);
        int bodyLength = tokenizer.tokenize(body).length;

        JString jString = (JString) fields[fieldPositions.get(KBARecord.FIELD_TITLE)];
        String title = jString.getValue();
        int titleLength = tokenizer.tokenize(title).length;

        jString = (JString) fields[fieldPositions.get(KBARecord.FIELD_ANCHOR)];
        int anchorLength = tokenizer.tokenize(jString.getValue()).length;

        // source
        jString = (JString) fields[fieldPositions.get(KBARecord.FIELD_SOURCE)];
        int source = ESource.getValueOfName(jString.getValue());

        // language detection
        jString = (JString) fields[fieldPositions.get(KBARecord.FIELD_LANGUAGE)];
        String language = jString.getValue();
        int lang = 0;

        if (language != null && language.equalsIgnoreCase("en"))
            lang = 1;
        
        // Generate results
        JRecord result = (JRecord) functionHelper.getResultObject();
        try {
            result.setField("doc_id", fields[fieldPositions.get(KBARecord.FIELD_DOCUMENT_ID)]);
            result.setField(EDocumentFeature.LENGTH_TITLE.getName(), setValue(JTypeTag.INT, titleLength));
            result.setField(EDocumentFeature.LENGTH_BODY.getName(), setValue(JTypeTag.INT, bodyLength));
            result.setField(EDocumentFeature.LENGTH_ANCHOR.getName(), setValue(JTypeTag.INT, anchorLength));
            result.setField(EDocumentFeature.SOURCE.getName(), setValue(JTypeTag.INT, source));
            result.setField(EDocumentFeature.LANGUAGE.getName(), setValue(JTypeTag.INT, lang));
        } catch (AsterixException e) {
            e.printStackTrace();
        }

        return result;
    }

}
