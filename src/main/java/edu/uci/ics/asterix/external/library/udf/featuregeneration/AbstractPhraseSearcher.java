package edu.uci.ics.asterix.external.library.udf.featuregeneration;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import edu.uci.ics.asterix.external.library.textanalysis.ITokenizer;
import edu.uci.ics.asterix.external.library.utils.StringUtil;

public abstract class AbstractPhraseSearcher implements IPhraseSearcher {
    public static final boolean STOPWORD_REMOVED = true;
    
    protected Map<String, Set<Posting>> phraseInvertedList;
    protected Set<String> phraseList;
    private int maxPhraseLength = 0;
    
    private String phrasesArray[][];
    
    
    protected Set<String> mentions;
    
    protected void buildInvertedList(Collection<String> phrases, ITokenizer tokenizer) {
        Set<Posting> postingList = null;
        phraseList = new HashSet<>();

        phrasesArray = new String[phrases.size()][];
        int index = 0;
        for (String phrase : phrases) {
            phrasesArray[index] = tokenizer.tokenize(phrase, STOPWORD_REMOVED);

            phraseList.add(StringUtil.concatenate(phrasesArray[index], ' '));

            if (phrasesArray[index].length > maxPhraseLength) {
                maxPhraseLength = phrasesArray[index].length;
            }

            for (int pos = 0; pos < phrasesArray[index].length; pos++) {
                String term = phrasesArray[index][pos];

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

    
    /**
     * Taking care of the postings for the posting list
     * @author heri
     *
     */
    public class Posting {
        int phraseIndex;
        int termPosition;
      
        
        public Posting(int phraseIndex, int termPosition) {
            this.phraseIndex = phraseIndex;
            this.termPosition = termPosition;
        }
        

        @Override
        public String toString() {
            return "<" + phraseIndex + "," + termPosition + ">";
        }


        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + phraseIndex;
            result = prime * result + termPosition;
            return result;
        }


        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            Posting other = (Posting) obj;
            return (other.phraseIndex==this.phraseIndex && 
                    other.termPosition==this.termPosition);
        }

    }

}
