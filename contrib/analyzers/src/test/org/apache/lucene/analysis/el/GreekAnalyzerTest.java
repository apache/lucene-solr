package org.apache.lucene.analysis.el;

/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;

import junit.framework.TestCase;


/**
 * A unit test class for verifying the correct operation of the GreekAnalyzer.
 *
 */
public class GreekAnalyzerTest extends TestCase {

	/**
	 * A helper method copied from org.apache.lucene.analysis.TestAnalyzers.
	 *
	 * @param a			the Analyzer to test
	 * @param input		an input String to analyze
	 * @param output	a String[] with the results of the analysis
	 * @throws Exception in case an error occurs
	 */
	private void assertAnalyzesTo(Analyzer a, String input, String[] output) throws Exception {
		TokenStream ts = a.tokenStream("dummy", new StringReader(input));
                final Token reusableToken = new Token();
		for (int i=0; i<output.length; i++) {
		        Token nextToken = ts.next(reusableToken);
			assertNotNull(nextToken);
			assertEquals(nextToken.term(), output[i]);
		}
		assertNull(ts.next(reusableToken));
		ts.close();
	}

	/**
	 * Test the analysis of various greek strings.
	 *
	 * @throws Exception in case an error occurs
	 */
	public void testAnalyzer() throws Exception {
		Analyzer a = new GreekAnalyzer();
		// Verify the correct analysis of capitals and small accented letters
		assertAnalyzesTo(a, "\u039c\u03af\u03b1 \u03b5\u03be\u03b1\u03b9\u03c1\u03b5\u03c4\u03b9\u03ba\u03ac \u03ba\u03b1\u03bb\u03ae \u03ba\u03b1\u03b9 \u03c0\u03bb\u03bf\u03cd\u03c3\u03b9\u03b1 \u03c3\u03b5\u03b9\u03c1\u03ac \u03c7\u03b1\u03c1\u03b1\u03ba\u03c4\u03ae\u03c1\u03c9\u03bd \u03c4\u03b7\u03c2 \u0395\u03bb\u03bb\u03b7\u03bd\u03b9\u03ba\u03ae\u03c2 \u03b3\u03bb\u03ce\u03c3\u03c3\u03b1\u03c2",
				new String[] { "\u03bc\u03b9\u03b1", "\u03b5\u03be\u03b1\u03b9\u03c1\u03b5\u03c4\u03b9\u03ba\u03b1", "\u03ba\u03b1\u03bb\u03b7", "\u03c0\u03bb\u03bf\u03c5\u03c3\u03b9\u03b1", "\u03c3\u03b5\u03b9\u03c1\u03b1", "\u03c7\u03b1\u03c1\u03b1\u03ba\u03c4\u03b7\u03c1\u03c9\u03bd",
				"\u03b5\u03bb\u03bb\u03b7\u03bd\u03b9\u03ba\u03b7\u03c3", "\u03b3\u03bb\u03c9\u03c3\u03c3\u03b1\u03c3" });
		// Verify the correct analysis of small letters with diaeresis and the elimination
		// of punctuation marks
		assertAnalyzesTo(a, "\u03a0\u03c1\u03bf\u03ca\u03cc\u03bd\u03c4\u03b1 (\u03ba\u03b1\u03b9)     [\u03c0\u03bf\u03bb\u03bb\u03b1\u03c0\u03bb\u03ad\u03c2]	-	\u0391\u039d\u0391\u0393\u039a\u0395\u03a3",
				new String[] { "\u03c0\u03c1\u03bf\u03b9\u03bf\u03bd\u03c4\u03b1", "\u03c0\u03bf\u03bb\u03bb\u03b1\u03c0\u03bb\u03b5\u03c3", "\u03b1\u03bd\u03b1\u03b3\u03ba\u03b5\u03c3" });
		// Verify the correct analysis of capital accented letters and capitalletters with diaeresis,
		// as well as the elimination of stop words
		assertAnalyzesTo(a, "\u03a0\u03a1\u039f\u03ab\u03a0\u039f\u0398\u0395\u03a3\u0395\u0399\u03a3  \u0386\u03c8\u03bf\u03b3\u03bf\u03c2, \u03bf \u03bc\u03b5\u03c3\u03c4\u03cc\u03c2 \u03ba\u03b1\u03b9 \u03bf\u03b9 \u03ac\u03bb\u03bb\u03bf\u03b9",
				new String[] { "\u03c0\u03c1\u03bf\u03c5\u03c0\u03bf\u03b8\u03b5\u03c3\u03b5\u03b9\u03c3", "\u03b1\u03c8\u03bf\u03b3\u03bf\u03c3", "\u03bc\u03b5\u03c3\u03c4\u03bf\u03c3", "\u03b1\u03bb\u03bb\u03bf\u03b9" });
	}

}
