package edu.uci.ics.asterix.external.library.udf.featuregeneration;

import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.external.udl.adapter.factory.KBARecord;
import edu.uci.ics.asterix.external.library.IFunctionHelper;
import edu.uci.ics.asterix.external.library.TopicEntity;
import edu.uci.ics.asterix.external.library.java.IJObject;
import edu.uci.ics.asterix.external.library.java.JObjects.JRecord;
import edu.uci.ics.asterix.external.library.java.JObjects.JString;
import edu.uci.ics.asterix.external.library.utils.LanguageDetector;

/**
 * Document features
 * 
 * @author Heri Ramampiaro <heri@idi.ntnu.no>
 * @author Krisztian Balog <krisztian.balog@idi.ntnu.no>
 */
public class DocumentFeature extends AbstractFeatureGenerator {

    private LanguageDetector languageDetector;
    private String admString;

    public DocumentFeature(HashMap<String, String> props) {
        super(props);
        languageDetector = new LanguageDetector();
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
        String source = doc.get(KBARecord.FIELD_SOURCE);
        
        String src = ESource.getValueOfName(source) +"";

        // language detection
        String language = doc.get(KBARecord.FIELD_LANGUAGE);
        String lang = "0";

        if (language!=null && language.equalsIgnoreCase("EN"))
            lang = "1";
        else if (languageDetector.isEnglish(title + " " + body))
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
        // Get the input record from the feed
        JRecord inputRecord = (JRecord) functionHelper.getArgument(0);
        
        IJObject[] fields = inputRecord.getFields();
        
        JString jString = (JString)fields[fieldPositions.get(KBARecord.FIELD_TITLE)];
        String title = jString.getValue();
        int titleLength = tokenizer.tokenize(title).length;
        
        jString = (JString)fields[fieldPositions.get(KBARecord.FIELD_BODY)];
        String body = jString.getValue();
        int bodyLength = tokenizer.tokenize(body).length;
        
        jString = (JString)fields[fieldPositions.get(KBARecord.FIELD_ANCHOR)];
        int anchorLength = tokenizer.tokenize(jString.getValue()).length;
        
        
        // source
        jString = (JString)fields[fieldPositions.get(KBARecord.FIELD_SOURCE)];
        String source = jString.getValue(); 

        // language detection
        jString = (JString)fields[fieldPositions.get(KBARecord.FIELD_LANGUAGE)];
        String language = jString.getValue();
        String lang = "0";

        if (language!=null && language.equalsIgnoreCase("EN"))
            lang = "1";
        else if (languageDetector.isEnglish(title + " " + body))
                lang = "1";
        
        // Generate results
        JRecord result = (JRecord) functionHelper.getResultObject();
        try {
            result.setField(EDocumentFeature.LENGTH_TITLE.getName(), new JString(titleLength+""));
            result.setField(EDocumentFeature.LENGTH_BODY.getName(), new JString(bodyLength + ""));
            result.setField(EDocumentFeature.LENGTH_ANCHOR.getName(), new JString(anchorLength + ""));
            result.setField(EDocumentFeature.SOURCE.getName(), new JString(ESource.getValueOfName(source) +""));
            result.setField(EDocumentFeature.LANGUAGE.getName(), new JString(lang + ""));
        } catch (AsterixException e) {
            e.printStackTrace();
        }

        return result;
    }

    
}
