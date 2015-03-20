package edu.uci.ics.asterix.external.library;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;

import edu.uci.ics.asterix.external.library.textanalysis.Tokenizer;
import edu.uci.ics.asterix.external.library.utils.StringUtil;
import edu.uci.ics.asterix.external.library.utils.TextAnalysis;

public class PhraseFinder {
    private Tokenizer tokenizer;

    private Map<String, Set<Posting>> phraseInvertedList;
    private Set<String> phraseList;
    private int maxPhraseLength = 0;
    private Set<String> mentions;

    public PhraseFinder(String[] phrases) {
        phraseInvertedList = new HashMap<>();
        this.tokenizer = Tokenizer.INSTANCE;
        mentions = new HashSet<>();

        buildInvertedList(Arrays.asList(phrases));
    }

    public PhraseFinder(Collection<String> phrases) {
        phraseInvertedList = new HashMap<>();
        this.tokenizer = Tokenizer.INSTANCE;
        mentions = new HashSet<>();
 
        buildInvertedList(phrases);

    }

    private void buildInvertedList(Collection<String> phrases) {
        Set<Posting> postingList = null;
        phraseList = new HashSet<>();

        int index = 0;
        for (String phrase : phrases) {
            String phraseTerms[] = tokenizer.tokenize(phrase, true); //TextAnalysis.analyze(ENTITY_ANALYZER, phrase);

            phraseList.add(StringUtil.concatenate(phraseTerms, ' '));

            if (phraseTerms.length > maxPhraseLength) {
                maxPhraseLength = phraseTerms.length;
            }

            for (int pos = 0; pos < phraseTerms.length; pos++) {
                String term = phraseTerms[pos];

                if (phraseInvertedList.containsKey(term)) {
                    postingList = phraseInvertedList.get(term);
                } else {
                    postingList = new HashSet<>();
                }
                postingList.add(new Posting(index, pos));
                phraseInvertedList.put(term, postingList);
            }
            index++;
        }

    }
    


    @Override
    public String toString() {
        if (phraseInvertedList == null)
            return null;

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

    public boolean containMention(String text) {
        return containMention(mentions, text);
    }
    
    public boolean containMention(Map<String, String> urlMap, Set<String> mentionList, String text) {
        mentionList.clear();
        String tokens[] = tokenizer.tokenize(text, true);
        
        StringBuilder candidatePhrase = new StringBuilder();
        boolean found=false;
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
                        mentionList.add(urlMap.get(token));
                        found = true;
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
                                    mentionList.add(urlMap.get(terms));
                                    found = true;
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
        return found;
        
    }

    public boolean containMention(Set<String> mentionList, String text) {
        mentionList.clear();
        String tokens[] = tokenizer.tokenize(text, true);
        StringBuilder candidatePhrase = new StringBuilder();
        boolean found=false;
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
                        mentionList.add(token);
                        found = true;
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
                                String terms = candidatePhrase.toString().intern();
                                if (phraseList.contains(terms)) {
                                    mentionList.add(terms);
                                    found = true;
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
        return found;
    }

    public Set<String> getMentions() {
        return mentions;
    }

    public static class Posting {
        int phraseIndex;
        int termPosition;

        public Posting(int pi, int tp) {
            phraseIndex = pi;
            termPosition = tp;
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

            if (obj == null || getClass() != obj.getClass())
                return false;

            Posting other = (Posting) obj;
            return (other.phraseIndex == phraseIndex && other.termPosition == termPosition);
        }

    }

}
