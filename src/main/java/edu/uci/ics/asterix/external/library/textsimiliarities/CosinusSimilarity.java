package edu.uci.ics.asterix.external.library.textsimiliarities;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.uci.ics.asterix.external.library.textanalysis.TextAnalyzer;

/**
 * A standalone cosinus similarity function
 * 
 * @author heri
 */

public class CosinusSimilarity extends TextSimilarity {

    public CosinusSimilarity(TextAnalyzer analyzer) {
        super(analyzer);
    }

    public CosinusSimilarity() {
        super();
    }

    @Override
    public double computeSimilarity(String text1, String text2) {
        setTermVectors(text1, text2);

        int len1 = termVector1.size();
        int len2 = termVector2.size();

        double scalar = 0, norm1 = 0, norm2 = 0;

        for (String term : allTerms) {
            /* Normalize the weights */
            double f = termVector1.containsKey(term) ? termVector1.get(term) : 0.0d;
            double w1 = f / (double) len1;
            f = termVector2.containsKey(term) ? termVector2.get(term) : 0.0d;
            double w2 = f / (double) len2;

            /* The scalar: vector products */
            scalar += w1 * w2;

            /* Base for the vector norms */
            norm1 += (w1 * w1);
            norm2 += (w2 * w2);

        }

        // Return the cosine similarity
        return (scalar / Math.sqrt(norm1 * norm2));
    }

    @Override
    public String getName() {
        return "Cosinus Similarity";
    }

}
