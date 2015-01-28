package edu.uci.ics.asterix.external.library.utils;

import java.io.IOException;
import java.util.Set;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

/**
 * Extend a @see org.apache.lucene.analysis.TokenFilter class to allow us remove
 * cumstom characters from entities
 * 
 * @author Heri Ramampiaro <heri@ntnu.no>
 * 
 */

public class EntityTokenFilter extends TokenFilter {
	private CharTermAttribute termAtt;
	private final boolean SKIP_NUMBERS = false; // Set to true if we want to
												// skip numbers

	
	protected EntityTokenFilter(TokenStream input) {
		super(input);
		termAtt = (CharTermAttribute) addAttribute(CharTermAttribute.class);
	}

    private char[] skipSpecialChars(char term_buffer[], int termbuff_len) {
        if (termbuff_len < 1)
            return term_buffer;
        char s = term_buffer[0];
        char e = term_buffer[termbuff_len - 1];
        
        if ( s == '(' || s == '"' ||
                e == ')' || e == '"' || e == '.' ||  e == ',') {
            int upto = 0;
            for (int i = 0; i < termbuff_len; i++) {
                char c = term_buffer[i];
                if (c != '(' && c != '"' && c != ')' && c != '.' && c != ',')
                    term_buffer[upto++] = c;
            }
            termAtt.setLength(upto);
        }
        return termAtt.buffer();
    }
	
    @Override
    public boolean incrementToken() throws IOException {
       
        while (input.incrementToken()) {
            char term_buffer[] = termAtt.buffer();
            int termbuff_len = termAtt.length();

            // Remove 's given that they are followed by a space
            if (termbuff_len > 1) {
                if (term_buffer[termbuff_len - 2] == '\'' && term_buffer[termbuff_len - 1] == 's') {
                    termAtt.setLength(termbuff_len - 2);
                    termbuff_len -= 2;
                }

                skipSpecialChars(term_buffer, termbuff_len);

                //prevTokenBuffer = new StringBuffer();

                // If we want to skip numbers
                if (SKIP_NUMBERS) {
                    if (!termAtt.toString().matches("\\d+"))
                        return true;
                } else
                    return true;
            }
        }
        return false;
    }
}
