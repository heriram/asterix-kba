package edu.uci.ics.asterix.external.library.udf;

import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.external.library.IExternalScalarFunction;
import edu.uci.ics.asterix.external.library.IFunctionHelper;
import edu.uci.ics.asterix.external.library.KBATopicEntityLoader;
import edu.uci.ics.asterix.external.library.PhraseFinder;
import edu.uci.ics.asterix.external.library.java.IJObject;
import edu.uci.ics.asterix.external.library.java.JObjects.JOrderedList;
import edu.uci.ics.asterix.external.library.java.JObjects.JRecord;
import edu.uci.ics.asterix.external.library.java.JObjects.JString;
import edu.uci.ics.asterix.external.library.java.JObjects.JUnorderedList;
import edu.uci.ics.asterix.external.library.java.JTypeTag;

public class KBAStreamFilteringFunction implements IExternalScalarFunction {
    private static final Logger LOGGER = Logger.getLogger(KBAStreamFilteringFunction.class.getName());
    private Set<String> nameVariants = null;
    private JUnorderedList mentionList;
    private Set<String> mentions;

    private boolean bodyContentIsList = true;

    private PhraseFinder searcher;

    @Override
    public void deinitialize() {
    }

    @Override
    public void initialize(IFunctionHelper functionHelper) throws Exception {
        LOGGER.info("Initializing KBAStreamFilteringFunction by loading the set of entities.");
        this.mentionList = new JUnorderedList(functionHelper.getObject(JTypeTag.STRING));

        this.mentions = new HashSet<>();

        // Load the entities into memory for faster access
        this.nameVariants = new HashSet<String>();
        KBATopicEntityLoader.loadNameVariants(this.nameVariants);
        this.searcher = new PhraseFinder(this.nameVariants);

        JRecord inputRecord = (JRecord) functionHelper.getArgument(0);
        switch (inputRecord.getFields()[3].getTypeTag()) {
            case ORDEREDLIST:
                bodyContentIsList = true;
                break;
            default:
                bodyContentIsList = false;
        }
    }

    /*
     * Checking entity mentions in a text
     */
    private void findEntities(IFunctionHelper functionHelper) throws Exception {
        if (nameVariants == null) {
            throw new Exception("Cannot start searching over a null or an empty set... "
                    + "Please initialize the name variant set first.");
        }

        this.mentionList.clear();
        JRecord inputRecord = (JRecord) functionHelper.getArgument(0);
        IJObject[] fields = inputRecord.getFields();

        JString titleText = (JString) fields[2]; // Title field

        IJObject bodyField = fields[3]; // Body field

        StringBuilder sb = new StringBuilder(titleText.getValue());
        if (bodyContentIsList) {
            int size = ((JOrderedList) bodyField).size();
            for (int i = 0; i < size; i++) {
                JString element = (JString) ((JOrderedList) bodyField).getElement(i);
                sb.append(" " + element.getValue());
            }
        } else { // It is assumed to be a JString
            sb.append(" " + ((JString) bodyField).getValue());
        }

        searcher.containMention(mentions, sb.toString());

        // Find all entities mentioned in the text
        for (String name : mentions) {
            JString newField = (JString) functionHelper.getObject(JTypeTag.STRING);
            newField.setValue(name);
            mentionList.add(newField);
        }

    }

    @Override
    public void evaluate(IFunctionHelper functionHelper) throws Exception {
        // Get the input
        JRecord inputRecord = (JRecord) functionHelper.getArgument(0);
        int num_found = 0;

        // Check entity mentions - store found entities in mentionList
        findEntities(functionHelper);

        num_found = mentionList.size();

        if (num_found > 0) {
            LOGGER.log(Level.INFO, "Mention list has " + num_found + " elements.");

            IJObject[] fields = inputRecord.getFields();
            int fieldLength = fields.length;

            if (fieldLength > 9) {
                inputRecord.setValueAtPos(9, mentionList);
                //inputRecord.setField("mentions", mentionList);

            } else {
                inputRecord.addField("mentions", mentionList);
            }
        }
        functionHelper.setResult(inputRecord);
    }

}
