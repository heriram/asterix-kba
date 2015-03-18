package edu.uci.ics.asterix.external.library.textsimiliarities;

import java.util.Arrays;
import java.util.HashSet;
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
        Set<String> termset1 = termVector1.keySet();
        Set<String> termset2 = termVector2.keySet();
        for (String term : termset1) {
            if (termset2.contains(term)) {
                count++;
            }
        }

        return count;
    }

    @Override
    public double computeSimilarity(String text1, String text2) {
        setTermVectors(text1, text2);

        // return the Jaccard similarity
        return ((double) (sizeOfIntersection()) / (double) (allTerms.size()));
    }

    @Override
    public String getName() {
        return "Jaccard Similarity";
    }
}
