package edu.uci.ics.asterix.external.library.utils;

import java.io.IOException;
import java.io.Reader;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.util.CharArraySet;

/**
 * A custom analyzer which basically does the same thing as a StandardAnalyzer
 * plus using i a custom EntityTokenFilter to remove char we don't need
 * 
 * @author Heri Ramampiaro <heri@ntnu.no>
 * 
 */

public class EntityAnalyzer extends Analyzer {
    protected Set<Object> stopWords;
    
	protected EntityAnalyzer() {
		super();
		this.stopWords = StopAnalyzer.ENGLISH_STOP_WORDS_SET;
	}

	protected EntityAnalyzer(Set<Object> sw) {
		super();
		this.stopWords = sw;
	}

	@Override
	protected TokenStreamComponents createComponents(String field, Reader reader) {

	    // Generate initial token stream
		final WhitespaceTokenizer src = new WhitespaceTokenizer(reader); // StandardTokenizer(reader);
		TokenStream result = new StandardFilter(src);
		// Lower all cases
		result = new LowerCaseFilter(result);
		// Remove stopwords
		result = new StopFilter(result,(CharArraySet) this.stopWords);
		// Use our own entity tokenfilter to remove chars we
        // don't need
		result = new EntityTokenFilter(result); 
		return new TokenStreamComponents(src, result) {
			@Override
			protected void setReader(final Reader reader) throws IOException {
				super.setReader(reader);
			}
		};

	}

}
