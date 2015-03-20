package edu.uci.ics.asterix.external.library.udf.featuregeneration;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import edu.uci.ics.asterix.external.library.textanalysis.ITokenizer;
import edu.uci.ics.asterix.external.library.utils.StringUtil;

public abstract class AbstractPhraseSearcher implements IPhraseSearcher {
    public static final boolean STOPWORD_REMOVED = true;
    
    private Map<String, Set<Posting>> phraseInvertedList;
    private Set<String> phraseList;
   
    private Map<String, String[]> phrasesMap;
    
    protected Set<String> mentions;
    
    protected void buildInvertedList(Collection<String> phrases, ITokenizer tokenizer) {
        Set<Posting> postingList = null;
        phraseList = new HashSet<>();

        
        int index = 0;
        phrasesMap = new HashMap<>();
        for (String phrase : phrases) {
            String phraseTokens[] = tokenizer.tokenize(phrase, STOPWORD_REMOVED);
            phrasesMap.put(phrase,  phraseTokens);
            phraseList.add(StringUtil.concatenate(phraseTokens, ' '));

            for (int pos = 0; pos < phraseTokens.length; pos++) {
                String term = phraseTokens[pos];

                if (phraseInvertedList.containsKey(term)) {
                    postingList = phraseInvertedList.get(term);
                } else {
                    postingList = new HashSet<Posting>();
                }
                postingList.add(new Posting(index, pos));
                phraseInvertedList.put(term, postingList);
            }
            index++;
        }

    }
        
    @Override
    public String toString() {
        Iterator<Entry<String, Set<Posting>>> it = phraseInvertedList.entrySet().iterator();
        StringBuilder sb = new StringBuilder();
        while (it.hasNext()) {
            Entry<String, Set<Posting>> entry = it.next();
            sb.append(entry.getKey() + ":");
            Set<Posting> postingList = entry.getValue();
            for (Posting p : postingList) {
                sb.append(" " + p.toString());
            }
            sb.append('\n');

        }

        return sb.toString();
    }
}
