package edu.uci.ics.asterix.external.library.textanalysis;

public interface ITokenizer {
    public String[] tokenize(String text);
    public String[] tokenize(String text, boolean removeStopWord);

}
