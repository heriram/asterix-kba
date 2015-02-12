package edu.uci.ics.asterix.external.library;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;

import edu.uci.ics.asterix.external.library.PhraseFinder;
import edu.uci.ics.asterix.external.library.utils.TextAnalysis;
import edu.uci.ics.asterix.external.library.IExternalScalarFunction;
import edu.uci.ics.asterix.external.library.IFunctionHelper;
import edu.uci.ics.asterix.external.library.java.JObjects.JOrderedList;
import edu.uci.ics.asterix.external.library.java.IJObject;
import edu.uci.ics.asterix.external.library.java.JTypeTag;
import edu.uci.ics.asterix.external.library.java.JObjects.JRecord;
import edu.uci.ics.asterix.external.library.java.JObjects.JString;
import edu.uci.ics.asterix.external.library.java.JObjects.JUnorderedList;

public class KBAStreamFilteringFunction implements IExternalScalarFunction {
    private Set<String> nameVariants = null;
    private JUnorderedList mentionList = null;
    
    @Override
    public void deinitialize() {
    }

    @Override
    public void initialize(IFunctionHelper functionHelper) throws Exception {
        mentionList = new JUnorderedList(functionHelper.getObject(JTypeTag.STRING));
        
        // Load the entities into memory for faster access
        nameVariants = new HashSet<String>();
        KBATopicEntityLoader.loadNameVariants(nameVariants);
    }

    
    private String getContent(JString titleContent, JOrderedList bodyContent) {
     // Concatenate the fields before searching
        StringBuilder sb = new StringBuilder(titleContent.getValue());
        
        for (int i=0; i<bodyContent.size(); i++) {
            sb.append(" ");
            sb.append(((JString)bodyContent.getElement(i)).getValue());
        }
        
        return sb.toString();
    }
    
    /*
     * Checking entity mentions in a text
     */
    private void findEntities(IFunctionHelper functionHelper, JRecord inputRecord, Set<String> nameVariants) throws Exception {
        if (nameVariants == null || nameVariants.isEmpty())
            throw new Exception("Cannot start searching over a null or an empty set... "
                    + "Please initialize the name variant set first.");
        
        IJObject[] fields = inputRecord.getFields();
        
        JString titleText = (JString) fields[2];  //.getValueByName("title_cleansed");
        JOrderedList bodyText = (JOrderedList) fields[3]; //inputRecord.getValueByName("body_cleansed");

        
        Analyzer analyzer = TextAnalysis.getAnalyzer();
        
        String content = getContent(titleText, bodyText);

        Map<String, Set<Integer>> analyzed_text = new HashMap<String, Set<Integer>>();
        TextAnalysis.analyze(analyzer, content, analyzed_text);

        // Find all entities mentioned in the text
        Iterator<String> nameVariantsIterator = nameVariants.iterator();
        while (nameVariantsIterator.hasNext()) {
            String name = nameVariantsIterator.next();
            if (PhraseFinder.find(analyzed_text, TextAnalysis.analyze(analyzer, name)) == true) {
                JString newField = (JString) functionHelper.getObject(JTypeTag.STRING);
                newField.setValue(name);
                mentionList.add(newField);
            }
        }

    }
    
    @Override
    public void evaluate(IFunctionHelper functionHelper) throws Exception {
        // Get the input
        JRecord inputRecord = (JRecord) functionHelper.getArgument(0);
        
        // Check entity mentions - store found entities in mentionList
        findEntities(functionHelper, inputRecord, nameVariants);

        inputRecord.addField("mentions", mentionList);

        functionHelper.setResult(inputRecord);
    }

}
