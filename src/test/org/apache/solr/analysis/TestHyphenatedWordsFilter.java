package org.apache.solr.analysis;

import java.io.IOException;
import java.io.StringReader;

import junit.framework.TestCase;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceTokenizer;

/**
 * HyphenatedWordsFilter test
 */
public class TestHyphenatedWordsFilter extends TestCase {
	public void testHyphenatedWords() throws Exception {
		String input = "ecologi-\r\ncal devel-\r\n\r\nop compre-\u0009hensive-hands-on";
		String outputAfterHyphenatedWordsFilter = "ecological develop comprehensive-hands-on";
		// first test
		TokenStream ts = new WhitespaceTokenizer(new StringReader(input));
		ts = new HyphenatedWordsFilter(ts);
		String actual = tsToString(ts);
		assertEquals("Testing HyphenatedWordsFilter",
				outputAfterHyphenatedWordsFilter, actual);
	}

	public static String tsToString(TokenStream in) throws IOException {
		StringBuffer out = new StringBuffer();
		Token t = in.next();
		if (null != t)
			out.append(t.termText());

		for (t = in.next(); null != t; t = in.next()) {
			out.append(" ").append(t.termText());
		}
		return out.toString();
	}
}
