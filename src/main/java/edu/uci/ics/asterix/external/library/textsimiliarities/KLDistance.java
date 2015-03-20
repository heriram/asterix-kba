package edu.uci.ics.asterix.external.library.textsimiliarities;

import java.util.Iterator;
import java.util.Set;

import edu.uci.ics.asterix.external.library.textanalysis.TextAnalyzer;

public class KLDistance extends KLDivergence {
    public static enum KLType {
        KLD,
        JENSEN,
        BIGI,
        BENNET,
        DIRICHLET;
    }


    private double beta1, beta2;

    private KLType type;

    public KLDistance() {
        super();
        this.type = KLType.KLD;
    }

    public KLDistance(KLType type) {
        super();
        this.type = type;
    }

    public KLDistance(TextAnalyzer analyzer) {
        super(analyzer);
        this.type = KLType.KLD;
    }

    public KLDistance(TextAnalyzer analyzer, KLType type) {
        super(analyzer);
        this.type = KLType.KLD;
        this.type = type;
    }

    private void computeBetaValues() {
        double sum1 = 0;
        double sum2 = 0;

        Set<String> termSet1 = termVector1.keySet();
        Set<String> termSet2 = termVector2.keySet();

        Iterator<String> it = allTerms.iterator();
        while (it.hasNext()) {
            String term = it.next();
            if (!termSet1.contains(term)) {
                sum1 += EPSILON;
            }
            if (!termSet2.contains(term)) {
                sum2 += EPSILON;
            }
        }

        beta1 = 1 - sum1;
        beta2 = 1 - sum2;
    }

    @Override
    protected void computeProbabilitiesOf(String term) {
        prob1 = termVector1.containsKey(term) ? beta1 * (termVector1.get(term) / (double) length1) : EPSILON;
        prob2 = termVector2.containsKey(term) ? beta2 * (termVector2.get(term) / (double) length2) : EPSILON;
    }

    /*
     * Compute standard KL distance
     */
    private double KLD(double p1, double p2) {
        return p1 * Math.log(p1 / p2) + p2 * Math.log(p2 / p1);
    }

    /*
     * Compute KL distance based on method by Bigi
     */
    private double bigiKLD(double p1, double p2) {
        return ((p1 - p2) * Math.log(p1 / p2));
    }

    /*
     * Compute KL distance based on method by Jensen
     */
    private double jensenKLD(double p1, double p2) {
        double prob12 = 0.5 * (p1 + p2);
        return (0.5 * (p1 * Math.log(p1 / prob12) + p2 * Math.log(p2 / prob12)));
    }
    
    
    /*
     * Compute KL distance using Dirichlet Smoothed Language model
     */
    private double dirichletSmoothedLMKLD(String term) {
        // smoothing param is set to the avg dl
        double mu = (length1+length2)/2d; 
        
        // Get the term frequencies
        int tf1 = termVector1.containsKey(term) ? termVector1.get(term):0;
        int tf2 = termVector2.containsKey(term) ? termVector2.get(term):0;
        
        // Compute term probability P(t)
        double pt = (tf1 + tf2)/(double)(length1+length2);
        
        // Compute term-doc probabilities P(t|d)
        prob1 = (tf1+mu*pt)/(length1+mu);
        prob2 = (tf2+mu*pt)/(length2+mu);   
        
        return (prob1 * (Math.log((prob1) / (prob2))));
    }

    @Override
    public double computeSimilarity(String text1, String text2) {
        buildTermVectors(text1, text2);
        computeBetaValues();

        double result = 0;

        for (String key : allTerms) {
            computeProbabilitiesOf(key);

            switch (type) {
                case JENSEN:
                    result += jensenKLD(prob1, prob2);
                    break;
                case BIGI:
                    result += bigiKLD(prob1, prob2);
                    break;
                case BENNET:
                    double kld = KLD(prob1, prob2);
                    if (result < kld) {
                        result = kld;
                    }
                    break;
                case DIRICHLET:
                    result += dirichletSmoothedLMKLD(key);
                    break;
                default:
                    result += KLD(prob1, prob2);
            }
        }

        return 1-result;
    }

    @Override
    public String getName() {
        char firstLetter = type.name().charAt(0);
        char buffer[] = type.name().toLowerCase().toCharArray();
        buffer[0] = firstLetter;
        
        return  new String(buffer) + "-based KL Divergence";
    }

}
