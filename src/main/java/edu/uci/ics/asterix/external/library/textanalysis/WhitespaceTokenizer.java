package edu.uci.ics.asterix.external.library.textanalysis;

import edu.uci.ics.asterix.external.library.utils.StringUtil;

public class WhitespaceTokenizer extends AbstractTokenizer {
    
    public static final WhitespaceTokenizer INSTANCE= new WhitespaceTokenizer();
    
    private WhitespaceTokenizer() {
        
    }

    @Override
    public String[] tokenize(String text) {
        int len = text.length();
        String[] temp = new String[len];
        int wordCount = 0;
        
        char wordBuff[] = new char[temp.length];
        int index = 0;

        for (int i = 0; i < len; i++) {
            char c = text.charAt(i);
            switch (c) {
                case ' ':
                case '\n':
                case '\t':
                case '\r':
                    if (index > 0) {
                        String word = new String(wordBuff, 0, index);
                        index = 0;
                        temp[wordCount] = word;
                        wordCount++;

                    }
                    break;
                default:
                    wordBuff[index] = c;
                        index++;
            }
        }
        
        temp[wordCount] = new String(wordBuff, 0, index).intern();
        wordCount++;
        String result[] = new String[wordCount];
        System.arraycopy(temp, 0, result, 0, wordCount);

        return result;
    }

    @Override
    public String[] tokenize(String text, boolean removeStopWord) {
        // TODO Auto-generated method stub
        return null;
    }

    public static void main(String[] args) {
        ITokenizer lexer = new WhitespaceTokenizer();
        String test = "@johnsmith: this is a test?\"    of this new tok?enizer -- It won't be bad if it works as good. BæøråäâïÄ99 19200?";

        String tokens[] = lexer.tokenize(test);

        System.out.println(StringUtil.concatenate(tokens, '|'));
        
    }
}
