package edu.uci.ics.asterix.external.library.textsimiliarities;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import edu.uci.ics.asterix.external.library.textanalysis.TextAnalyzer;

public class JaccardSimilarity extends TextSimilarity {

    public JaccardSimilarity() {
        super();
    }

    public JaccardSimilarity(TextAnalyzer analyzer) {
        super(analyzer);

    }

    private double sizeOfIntersection() {
        double count = 0;
        Set<String> termset2 = termVector2.keySet();
        
        Iterator<String> it = termVector1.keySet().iterator();
        while(it.hasNext()) {
            if (termset2.contains(it.next())) {
                count++;
            }
        }

        return count;
    }

    @Override
    public double computeSimilarity(String text1, String text2) {
        buildTermVectors(text1, text2);
        // return the Jaccard similarity
        return ((double) (sizeOfIntersection()) / (double) (allTerms.size()));
    }

    @Override
    public String getName() {
        return "Jaccard Similarity";
    }
}
