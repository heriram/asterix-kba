package edu.uci.ics.asterix.external.library.textanalysis;

public class TextAnalyzer  {
    private Tokenizer tokenizer;
    
    public TextAnalyzer() {
        this.tokenizer = Tokenizer.INSTANCE;
    }
    
    ITokenizer getTokenizer() {
        return this.tokenizer;
    }
    
    public String[] analyze(String text) {
        return tokenizer.tokenize(text);
    }

}
