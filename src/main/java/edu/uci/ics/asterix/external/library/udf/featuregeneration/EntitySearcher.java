package edu.uci.ics.asterix.external.library.udf.featuregeneration;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.uci.ics.asterix.external.library.textanalysis.ITokenizer;
import edu.uci.ics.asterix.external.library.textanalysis.Tokenizer;

public class EntitySearcher extends AbstractPhraseSearcher {
    private ITokenizer tokenizer;
    private EntityInvertedList entityInvertedList;

    private int entityPositions[];

    public EntitySearcher(String[] phrases) {
        this(Arrays.asList(phrases));
    }

    public EntitySearcher(Collection<String> phrases) {
        this.tokenizer = Tokenizer.INSTANCE;
        entityInvertedList = new EntityInvertedList(phrases, tokenizer);
        //buildInvertedList(phrases, tokenizer);
        mentions = new HashSet<String>();

    }

    public int numOfMentionedEntities(String text, Set<String> nameSet) {
        // Get the subinverted list for this group of names
        Map <String, Set<Posting>> subInvertedList= entityInvertedList.getSubInvertedList(nameSet);
        
        if (nameSet == null || subInvertedList == null)
            return 0; //TODO Alternatively we could throw an exception here.

        String tokens[] = ((Tokenizer) tokenizer).tokenize(text, STOPWORD_REMOVED);
        StringBuilder candidatePhrase = new StringBuilder();

        mentions.clear();
        try {
            Set<Posting> prevPost = null;
            int count = 0;
            for (int t = 0; t < tokens.length; t++) {

                // Check if the topics contain current token
                if (subInvertedList.containsKey(tokens[t])) {
                    // Check if the current token is a valid word
                    if (count == 0) {
                        candidatePhrase = new StringBuilder(tokens[t]);
                        count++;
                    }

                    // Check if current token is part of the dictionary
                    if (nameSet.contains(tokens[t])) {
                        mentions.add(tokens[t]);
                    }

                    // Is the token part of a phrase ?
                    // Get the posting list for this token
                    Set<Posting> currPostList = subInvertedList.get(tokens[t]);
                    for (Posting currPost : currPostList) {
                        if (currPost.termPosition > 0 && prevPost != null) {
                            Posting prev = new Posting(currPost.phraseIndex, (currPost.termPosition - 1));
                            if (prevPost.contains(prev)) {
                                candidatePhrase.append(" " + tokens[t]);
                                count++;
                                String terms = candidatePhrase.toString();
                                if (nameSet.contains(terms)) {
                                    mentions.add(terms);
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

        return mentions.size();
    }

    @Override
    public boolean containMention(String text) {
        return containMention(text, null, this.mentions);

    }
    
    public boolean containMention(String text,  Map<String, String> urlMap) {
        return containMention(text, urlMap, this.mentions);
    }
    
    public boolean containMention(String text,  Set<String> mentions) {
        return containMention(text, null, mentions);
    }
    
    public boolean containMention(String text,  Map<String, String> urlMap, Set<String> mentions) {
        mentions.clear();
        String tokens[] = ((Tokenizer) tokenizer).tokenize(text, STOPWORD_REMOVED);
        StringBuilder candidatePhrase = new StringBuilder();

        entityPositions = new int[tokens.length];
        int pos = 0;

        try {
            Set<Posting> prevPost = null;
            int count = 0;
            for (int t = 0; t < tokens.length; t++) {

                // Check if the topics contain current token
                if (entityInvertedList.containsTerm(tokens[t])) {
                    // Check if the current token is a valid word
                    if (count == 0) {
                        candidatePhrase = new StringBuilder(tokens[t]);
                        count++;
                    }

                    // Check if current token is part of the dictionary
                    if (entityInvertedList.containsPhrase(tokens[t])) {
                        if (urlMap==null)
                            mentions.add(tokens[t]);
                        else 
                            mentions.add(urlMap.get(tokens[t]));
                        
                        entityPositions[pos] = t;
                        pos++;
                    }

                    // Is the token part of a phrase ?
                    // Get the posting list for this token
                    Set<Posting> currPostList = entityInvertedList.getPostingList(tokens[t]);
                    for (Posting currPost : currPostList) {
                        if (currPost.termPosition > 0 && prevPost != null) {
                            Posting prev = new Posting(currPost.phraseIndex, (currPost.termPosition - 1));
                            if (prevPost.contains(prev)) {
                                candidatePhrase.append(" " + tokens[t]);
                                count++;
                                String terms = candidatePhrase.toString();
                                if (entityInvertedList.containsPhrase(terms)) {
                                    if (urlMap==null)
                                        mentions.add(terms);
                                    else 
                                        mentions.add(urlMap.get(terms));
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
