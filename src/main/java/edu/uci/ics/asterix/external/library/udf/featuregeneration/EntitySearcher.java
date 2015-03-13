package edu.uci.ics.asterix.external.library.udf.featuregeneration;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import edu.uci.ics.asterix.external.library.textanalysis.ITokenizer;
import edu.uci.ics.asterix.external.library.textanalysis.Tokenizer;

public class EntitySearcher extends AbstractPhraseSearcher {
    private ITokenizer tokenizer;
    
    private int entityPositions[];
     
    public EntitySearcher(String[] phrases) {
        phraseInvertedList = new HashMap<String, Set<Posting>>();
        this.tokenizer= Tokenizer.INSTANCE;
        buildInvertedList(Arrays.asList(phrases), tokenizer);
        mentions = new HashSet<String>();
        
    }

    public EntitySearcher(Collection<String> phrases) {
        phraseInvertedList = new HashMap<String, Set<Posting>>();
        
        this.tokenizer= Tokenizer.INSTANCE;
        buildInvertedList(phrases, tokenizer);
        mentions = new HashSet<String>();
        
    }
    
    @Override
    public boolean containMention(String text) {
        mentions.clear();
        String tokens[] = ((Tokenizer)tokenizer).tokenize(text, STOPWORD_REMOVED); 
        StringBuilder candidatePhrase = new StringBuilder();
        
        entityPositions = new int[tokens.length];
        int pos = 0;

        try {
            Set<Posting> prevPost = null;
            int count = 0;
            for (int t = 0; t < tokens.length; t++) {
                String token = new String(tokens[t]);

                // Check if the topics contain current token
                if (phraseInvertedList.containsKey(token)) {
                    // Check if the current token is a valid word
                    if (count == 0) {
                        candidatePhrase = new StringBuilder(token);
                        count++;
                    }

                    // Check if current token is part of the dictionary
                    if (phraseList.contains(token)) {
                        mentions.add(token);
                        entityPositions[pos] = t;
                        pos++;
                    }

                    // Is the token could be part of a phrase ?
                    // Get the posting list for this token
                    Set<Posting> currPostList = phraseInvertedList.get(token);
                    for (Posting currPost : currPostList) {
                        if (currPost.termPosition > 0 && prevPost != null) {
                            Posting prev = new Posting(currPost.phraseIndex, (currPost.termPosition - 1));
                            if (prevPost.contains(prev)) {
                                candidatePhrase.append(" " + token);
                                count++;
                                String terms = candidatePhrase.toString();
                                if (phraseList.contains(terms)) {
                                    mentions.add(terms);
                                    entityPositions[pos] = t;
                                    pos++;
                                }
                            } else {
                                candidatePhrase.setLength(0);
                                count = 0;

                            }
                        }
                    }

                    if (count != 0) {
                        prevPost = currPostList;
                    } else {
                        prevPost = null;
                    }

                } else {
                    // If one term or more terms found but not constituting a valid phrase
                    count = 0;
                    candidatePhrase.setLength(0);
                    prevPost = null;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // Help the GC...
            candidatePhrase = null;
            tokens = null;
        }

        return !mentions.isEmpty();
    }

    @Override
    public Set<String> getMentions() {
        return this.mentions;
    }

    
    @Override
    public int[] search(String text) {
        if (containMention(text))
            return entityPositions;
        return null;
    }

}
