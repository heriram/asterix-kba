package edu.uci.ics.asterix.external.library;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;

import edu.uci.ics.asterix.external.library.utils.TextAnalysis;

public class PhraseFinder {

    public static boolean find(String text, String phrase) throws Exception {
        return find(TextAnalysis.getAnalyzer(), text, phrase);
    }

    public static boolean find(Map<String, Set<Integer>> analyzed_text, String[] phrase_terms) {
        // No search terms
        if (phrase_terms.length == 0)
            return false;

        // If only one term, no need to do phrase search
        if (phrase_terms.length == 1)
            return analyzed_text.containsKey(phrase_terms[0]);

        // For two or more terms
        if (!analyzed_text.containsKey(phrase_terms[0]))
            return false;

        /* Assuming terms[i] has been found, then check the next term */

        // Get the positions of the previous term
        Set<Integer> pos_prev = analyzed_text.get(phrase_terms[0]);
        for (int i = 1; i < phrase_terms.length; i++) {
            if (!analyzed_text.containsKey(phrase_terms[i]))
                return false;

            // Get the positions of the current term
            Set<Integer> pos_curr = analyzed_text.get(phrase_terms[i]);
            boolean consecutive = false;
            for (int pos : pos_curr) {
                if (pos_prev.contains(pos - 1)) {
                    consecutive = true;
                    break;
                }
            }

            if (!consecutive)
                return false;

            // Move on to the next term
            pos_prev = pos_curr;
        }

        // Reaching this far meaning that we have a hit
        return true;
    }

    public static boolean find(Analyzer analyzer, String text, String phrase) throws Exception {
        String terms[] = TextAnalysis.analyze(analyzer, phrase);
        Map<String, Set<Integer>> analyzed = new HashMap<String, Set<Integer>>();
        TextAnalysis.analyze(analyzer, text, analyzed);
        
        return find(analyzed, terms);

    }

}
