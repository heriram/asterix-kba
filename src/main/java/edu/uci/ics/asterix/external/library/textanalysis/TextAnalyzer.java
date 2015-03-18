package edu.uci.ics.asterix.external.library.textanalysis;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class TextAnalyzer  {
    public static class Term {
        String term;
        int frequence;
        
        
        public Term(String term, int frequence) {
            this.term = term;
            this.frequence = frequence;
        }
        
        @Override
        public String toString() {
            return "{"+term+": " + frequence +"}";
        }
    }
    
    private int length;
    private int maxTf=0;

    private Map<String, Integer> termVector;
    
    private ITokenizer tokenizer;
    private Term tokens[];
    private String terms[];
    
    private PorterStemmer stemmer;
    
    public TextAnalyzer() {
        this(Tokenizer.INSTANCE, new PorterStemmer());
    }
    
    public TextAnalyzer(ITokenizer tokenizer) {
        this(tokenizer, new PorterStemmer());
    }
    
    public TextAnalyzer(PorterStemmer stemmer) {
        this(Tokenizer.INSTANCE, new PorterStemmer());
    }
    
    public TextAnalyzer(ITokenizer tokenizer, PorterStemmer stemmer) {
        this.tokenizer = tokenizer;
        termVector = new HashMap<>();
        this.stemmer = stemmer;
    }
    
    ITokenizer getTokenizer() {
        return this.tokenizer;
    }
    
    public Term[] getTokens() {
        return tokens;
    }

    public String[] getTerms() {
        return terms;
    }
    
    
    public String[] getAnalyzedTerms() {
        String t[] = new String[termVector.size()];
        Iterator<String> it = termVector.keySet().iterator();
       
        for (int i=0; i<t.length && it.hasNext(); i++)
            t[i] = it.next();
        
        return t;
    }
    
    public void getTerms(Set<String> termSet) {
        termSet.addAll(termVector.keySet());
    }
    
    public void getTermVector(Map<String,Integer> termVector) {
        termVector.putAll(this.termVector);
    }
    
    public Integer[] getRawTermFrequencies() {
        return termVector.values().toArray(new Integer[termVector.size()]);
    }
    
    public double[] getTermNormalizedFrequencies() {
        List<Integer> tfList = (List<Integer>) termVector.values();
         
        int size = tfList.size();
        double ntfs[] = new double[size];
        
        for (int i=0; i<size; i++) {
            ntfs[i] = tfList.get(i) / (double)maxTf;
        }
        return ntfs;
    }
    
    public void reset() {
        termVector.clear();
        maxTf = 0;
        length = 0;
    }
    
    /** 
     * Help the GC
     */
    public void deallocate() {
        terms = null;
        termVector = null;      
        tokenizer = null;
        tokens = null;
        terms = null;
    }
    
    public int getLength() {
        return length;
    }
    
    
    public void analyze(String text) {
        terms =  tokenizer.tokenize(text);
        reset();
        length = terms.length;
        
        for(String t: terms) {
            int f = 1;
            String term = this.stemmer.stem(t);
            
            if(termVector.containsKey(term))
                f += termVector.get(term);
            
            if (f>maxTf) 
                maxTf = f;
            
            termVector.put(term, f);
        }
        tokens = new Term[termVector.size()];
        
        Iterator<Entry<String, Integer>> it = termVector.entrySet().iterator();
        int i=0;
        while(it.hasNext()) {
            Entry<String, Integer> e = it.next();  
            tokens[i] = new Term (e.getKey(), e.getValue());
            i++;
        }
    }

}
