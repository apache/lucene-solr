package org.apache.lucene.analysis.de;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import java.io.IOException;
import java.util.Hashtable;

/**
 * A filter that stemms german words. It supports a table of words that should
 * not be stemmed at all.
 *
 * @author    Gerhard Schwarz
 * @version   $Id$
 */
public final class GermanStemFilter extends TokenFilter {

	/**
	 * The actual token in the input stream.
	 */
	private Token token = null;
	private GermanStemmer stemmer = null;
	private Hashtable exclusions = null;
	
	public GermanStemFilter( TokenStream in ) {
		stemmer = new GermanStemmer();
		input = in;
	}
	
	/**
	 * Builds a GermanStemFilter that uses an exclusiontable.
	 */
	public GermanStemFilter( TokenStream in, Hashtable exclusiontable ) {
		this( in );
		this.exclusions = exclusions;
	}

	/**
	 * @return  Returns the next token in the stream, or null at EOS.
	 */
	public final Token next()
		throws IOException {
		if ( ( token = input.next() ) == null ) {
			return null;
		}
		// Check the exclusiontable.
		else if ( exclusions != null && exclusions.contains( token.termText() ) ) {
			return token;
		}
		else {
			String s = stemmer.stem( token.termText() );
			// If not stemmed, dont waste the time creating a new token.
			if ( !s.equals( token.termText() ) ) {
				return new Token( s, 0, s.length(), token.type() );
			}
			return token;
		}
	}
}


