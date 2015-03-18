package edu.uci.ics.asterix.external.library.textsimiliarities;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.uci.ics.asterix.external.library.textanalysis.TextAnalyzer;

public abstract class TextSimilarity {
    protected TextAnalyzer analyzer;
    protected Set<String> allTerms = null;
    protected Map<String, Integer> termVector1;
    protected Map<String, Integer> termVector2;
    protected int length1;
    protected int length2;

    protected TextSimilarity() {
        this.analyzer = new TextAnalyzer();
        this.allTerms = new HashSet<String>();
        this.termVector1 = new HashMap<String, Integer>(); 
        this.termVector2 = new HashMap<String, Integer>(); 
    }
    
    protected TextSimilarity(TextAnalyzer analyzer) {
        this.analyzer = analyzer;
        this.allTerms = new HashSet<String>();
        this.termVector1 = new HashMap<String, Integer>(); 
        this.termVector2 = new HashMap<String, Integer>(); 
    }
    
    protected void reset() {
        termVector1.clear();
        termVector2.clear();
        allTerms.clear();
    }
    /**
     *  Analyze text using a specific Analyzer with term frequency
     *  
     * @param text1
     * @param text2
     */
    protected void setTermVectors(String text1, String text2) {
        reset();
        analyzer.analyze(text1);
        length1 = analyzer.getLength();
        analyzer.getTermVector(termVector1);
        analyzer.getTerms(allTerms);
        
        analyzer.analyze(text2);
        length2 = analyzer.getLength();
        analyzer.getTermVector(termVector2);
        analyzer.getTerms(allTerms);
    }

    public abstract double computeSimilarity(String text1, String text2);
    
    public abstract String getName();
}
