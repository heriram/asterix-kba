package edu.uci.ics.asterix.external.library.textanalysis;

public class TextAnalyzer  {
    private Tokenizer tokenizer;
    
    public TextAnalyzer() {
        this.tokenizer = new Tokenizer();
    }
    
    ITokenizer getTokenizer() {
        return this.tokenizer;
    }
    
    public String[] analyze(String text) {
        return tokenizer.removeStopWord(tokenizer.tokenize(text.trim().toCharArray()));
    }

}
