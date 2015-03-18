package edu.uci.ics.asterix.external.library.textsimiliarities;

import java.util.Map;
import java.util.Set;

import edu.uci.ics.asterix.external.library.textanalysis.TextAnalyzer;

/**
 * Calculates the KL divergence between the two distributions.
 * That is, it calculates KL(from || to).
 * In other words, how well can d1 be represented by d2.
 * if there is some value in d1 that gets zero prob in d2, then return positive infinity.
 *
 * @author Heri Ramampiaro <heri@ntnu.no>
 */

public class KLDivergence extends TextSimilarity {
    private final double EPSILON = 1e-10;
    private final double LOG2 = Math.log(2.0);

    public KLDivergence() {
        super();
    }

    public KLDivergence(TextAnalyzer analyzer) {
        super(analyzer);
    }

    private double[] probabilitiesOf(String term) {
        double probs[] = new double[2];
        probs[0] = termVector1.containsKey(term) ? termVector1.get(term) / (double) length1 : EPSILON;
        probs[1] = termVector2.containsKey(term) ? termVector2.get(term) / (double) length2 : EPSILON;
        return probs;
    }

    @Override
    public double computeSimilarity(String text1, String text2) {
        setTermVectors(text1, text2);

        int numKeysRemaining = termVector1.size();
        double result = 0.0;
        double assignedMass1 = 0.0;
        double assignedMass2 = 0.0;

        double p1, p2;

        for (String key : allTerms) {
            double probs[] = probabilitiesOf(key);
            p1 = probs[0];
            p2 = probs[1];

            numKeysRemaining--;
            assignedMass1 += p1;
            assignedMass2 += p2;
            
            if (p1 >= EPSILON) {
                double logFract = Math.log(p1 / p2);
                if (logFract == Double.POSITIVE_INFINITY) {
                    return Double.POSITIVE_INFINITY; // can't recover
                }
                result += p1 * (logFract / LOG2); // express it in log base 2
            }
        }

        if (numKeysRemaining != 0) {
            p1 = (1.0 - assignedMass1) / numKeysRemaining;
            if (p1 > EPSILON) {
                p2 = (1.0 - assignedMass2) / numKeysRemaining;
                double logFract = Math.log(p1 / p2);
                if (logFract == Double.POSITIVE_INFINITY) {
                    return Double.POSITIVE_INFINITY; // can't recover
                }
                result += numKeysRemaining * p1 * (logFract / LOG2); // express it in log base 2
            }
        }
        return 1-result;
    }

    @Override
    public String getName() {
        return "KL Divergence";
    }

}
