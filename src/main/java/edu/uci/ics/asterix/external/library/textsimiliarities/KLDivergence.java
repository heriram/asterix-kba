package edu.uci.ics.asterix.external.library.textsimiliarities;

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
    protected final double EPSILON = 1e-10;
    protected final double LOG2 = Math.log(2.0);

    protected double prob1, prob2;
    
    public KLDivergence() {
        super();
    }

    public KLDivergence(TextAnalyzer analyzer) {
        super(analyzer);
    }

    protected void computeProbabilitiesOf(String term) {
        prob1 = termVector1.containsKey(term) ? termVector1.get(term) / (double) length1 : EPSILON;
        prob2 = termVector2.containsKey(term) ? termVector2.get(term) / (double) length2 : EPSILON;
    }

    
    @Override
    public double computeSimilarity(String text1, String text2) {
        buildTermVectors(text1, text2);

        int numKeysRemaining = termVector1.size();
        double result = 0.0;
        double assignedMass1 = 0.0;
        double assignedMass2 = 0.0;

        for (String key : allTerms) {
            computeProbabilitiesOf(key);
           
            numKeysRemaining--;
            assignedMass1 += prob1;
            assignedMass2 += prob2;
            
            if (prob1 >= EPSILON) {
                double logFract = Math.log(prob1 / prob2);
                if (logFract == Double.POSITIVE_INFINITY) {
                    return Double.POSITIVE_INFINITY; // can't recover
                }
                result += prob1 * (logFract / LOG2); // express it in log base 2
            }
        }

        if (numKeysRemaining != 0) {
            prob1 = (1.0 - assignedMass1) / numKeysRemaining;
            if (prob1 > EPSILON) {
                prob2 = (1.0 - assignedMass2) / numKeysRemaining;
                double logFract = Math.log(prob1 / prob2);
                if (logFract == Double.POSITIVE_INFINITY) {
                    return Double.POSITIVE_INFINITY; // can't recover
                }
                result += numKeysRemaining * prob1 * (logFract / LOG2); // express it in log base 2
            }
        }
        return 1-result;
    }

    @Override
    public String getName() {
        return "KL Divergence";
    }

}
