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

    /*
     * Checking entity mentions in a text
     */
    private void findEntities(IFunctionHelper functionHelper, JRecord inputRecord, Set<String> nv) throws Exception {
        if (nv == null || nv.isEmpty())
            throw new Exception("Cannot start searching over a null or an empty set... "
                    + "Please initialize the name variant set first.");
        
        JString title_text = (JString) inputRecord.getValueByName("title_cleansed");
        JString body_text = (JString) inputRecord.getValueByName("body_cleansed");

        Iterator<String> it = nv.iterator();

        Analyzer analyzer = TextAnalysis.getAnalyzer();
        
        // Concatenate the fields before searching
        StringBuilder sb = new StringBuilder(title_text.getValue());
        sb.append(" ").append(body_text.getValue());

        Map<String, Set<Integer>> analyzed_text = new HashMap<String, Set<Integer>>();
        TextAnalysis.analyze(analyzer, sb.toString(), analyzed_text);

        // Find all entities mentioned in the text
        while (it.hasNext()) {
            String name = it.next();
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
