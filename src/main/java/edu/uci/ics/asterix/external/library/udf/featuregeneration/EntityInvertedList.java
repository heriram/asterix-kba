package edu.uci.ics.asterix.external.library.udf.featuregeneration;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import edu.uci.ics.asterix.external.library.textanalysis.ITokenizer;
import edu.uci.ics.asterix.external.library.utils.StringUtil;
import edu.uci.ics.asterix.external.library.utils.Util;

public class EntityInvertedList {
    private Map<String, Set<Posting>> invertedList;
    private Map<String, Set<Posting>> subInvertedList;
    private Map<String, Set<String>> phrasesMap;
    private Set<String> analyzedPhraseList;
    private ITokenizer tokenizer;
    
    public EntityInvertedList(String [] phrases, ITokenizer tokenizer) {
        this(Arrays.asList(phrases), tokenizer);
    }
    public EntityInvertedList(Collection<String> phrases, ITokenizer tokenizer) {
        this.invertedList = new HashMap<>();
        this.subInvertedList = new HashMap<>();
        this.phrasesMap = new HashMap<>();
        this.analyzedPhraseList = new HashSet<>();
        this.tokenizer = tokenizer;
        
        initialize(phrases);
    }
    
    
    private void initialize(Collection<String> phrases) {
        Set<Posting> postingList = null;
        Iterator<String> it = phrases.iterator();
        
        int index=0;
        while(it.hasNext()) {
            String phrase = it.next();
            String tokens[] = tokenizer.tokenize(phrase);
            Set<String> tokenSet = new HashSet<>();
            Util.addAll(tokenSet, tokens);  
            phrase = StringUtil.concatenate(tokens, ' ');
            phrasesMap.put(phrase,  tokenSet);
            
            analyzedPhraseList.add(phrase);

            for (int pos = 0; pos < tokens.length; pos++) {
                String term = tokens[pos];

                if (invertedList.containsKey(term)) {
                    postingList = invertedList.get(term);
                } else {
                    postingList = new HashSet<Posting>();
                }
                postingList.add(new Posting(index, pos));
                invertedList.put(term, postingList);
            }
            index++;
        }      
    }
    
    public Map<String, Set<Posting>> getSubInvertedList(Collection<String> groupOfphrases) {
        subInvertedList.clear();
        for(String phrase : groupOfphrases) {
            Set<String> phraseTokenSet=phrasesMap.get(phrase);
            for(String term : phraseTokenSet)
                subInvertedList.put(term, invertedList.get(term));
        }
        
        return subInvertedList;
    }
    
    public boolean containsPhrase(String phrase) {
        return analyzedPhraseList.contains(phrase);
    }
    
    public boolean containsTerm(String term) {
        return invertedList.containsKey(term);
    }
    
    public Map<String, Set<Posting>> getInvertedList() {
        return this.invertedList;
    }
    
    public  Set<Posting> getPostingList(String term) {
        return invertedList.get(term);
    }
    
    public Set<String> getTokenSet(String phrase) {
        return this.phrasesMap.get(phrase);
    }
    
    public void clear() {
        invertedList.clear();
        phrasesMap.clear();
    }
    
    public void close() {
        invertedList = null;
        phrasesMap = null;
    }
    

}
