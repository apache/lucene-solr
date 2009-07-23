package org.apache.lucene.analysis.fr;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2004 The Apache Software Foundation.  All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution,
 *    if any, must include the following acknowledgment:
 *       "This product includes software developed by the
 *        Apache Software Foundation (http://www.apache.org/)."
 *    Alternately, this acknowledgment may appear in the software itself,
 *    if and wherever such third-party acknowledgments normally appear.
 *
 * 4. The names "Apache" and "Apache Software Foundation" and
 *    "Apache Lucene" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    "Apache Lucene", nor may "Apache" appear in their name, without
 *    prior written permission of the Apache Software Foundation.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 */

import java.io.StringReader;

import junit.framework.TestCase;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;

/**
 * Test case for FrenchAnalyzer.
 *
 * @version   $version$
 */

public class TestFrenchAnalyzer extends TestCase {

	// Method copied from TestAnalyzers, maybe should be refactored
	public void assertAnalyzesTo(Analyzer a, String input, String[] output)
		throws Exception {

		TokenStream ts = a.tokenStream("dummy", new StringReader(input));

                final Token reusableToken = new Token();
		for (int i = 0; i < output.length; i++) {
			Token nextToken = ts.next(reusableToken);
			assertNotNull(nextToken);
			assertEquals(nextToken.term(), output[i]);
		}
		assertNull(ts.next(reusableToken));
		ts.close();
	}

	public void testAnalyzer() throws Exception {
		FrenchAnalyzer fa = new FrenchAnalyzer();
	
		// test null reader
		boolean iaeFlag = false;
		try {
			fa.tokenStream("dummy", null);
		} catch (IllegalArgumentException iae) {
			iaeFlag = true;
		}
		assertEquals(iaeFlag, true);

		// test null fieldname
		iaeFlag = false;
		try {
			fa.tokenStream(null, new StringReader("dummy"));
		} catch (IllegalArgumentException iae) {
			iaeFlag = true;
		}
		assertEquals(iaeFlag, true);

		assertAnalyzesTo(fa, "", new String[] {
		});

		assertAnalyzesTo(
			fa,
			"chien chat cheval",
			new String[] { "chien", "chat", "cheval" });

		assertAnalyzesTo(
			fa,
			"chien CHAT CHEVAL",
			new String[] { "chien", "chat", "cheval" });

		assertAnalyzesTo(
			fa,
			"  chien  ,? + = -  CHAT /: > CHEVAL",
			new String[] { "chien", "chat", "cheval" });

		assertAnalyzesTo(fa, "chien++", new String[] { "chien" });

		assertAnalyzesTo(
			fa,
			"mot \"entreguillemet\"",
			new String[] { "mot", "entreguillemet" });

		// let's do some french specific tests now	

		/* 1. couldn't resist
		 I would expect this to stay one term as in French the minus 
		sign is often used for composing words */
		assertAnalyzesTo(
			fa,
			"Jean-François",
			new String[] { "jean", "françois" });

		// 2. stopwords
		assertAnalyzesTo(
			fa,
			"le la chien les aux chat du des à cheval",
			new String[] { "chien", "chat", "cheval" });

		// some nouns and adjectives
		assertAnalyzesTo(
			fa,
			"lances chismes habitable chiste éléments captifs",
			new String[] {
				"lanc",
				"chism",
				"habit",
				"chist",
				"élément",
				"captif" });

		// some verbs
		assertAnalyzesTo(
			fa,
			"finissions souffrirent rugissante",
			new String[] { "fin", "souffr", "rug" });

		// some everything else
		// aujourd'hui stays one term which is OK
		assertAnalyzesTo(
			fa,
			"C3PO aujourd'hui oeuf ïâöûàä anticonstitutionnellement Java++ ",
			new String[] {
				"c3po",
				"aujourd'hui",
				"oeuf",
				"ïâöûàä",
				"anticonstitutionnel",
				"jav" });

		// some more everything else
		// here 1940-1945 stays as one term, 1940:1945 not ?
		assertAnalyzesTo(
			fa,
			"33Bis 1940-1945 1940:1945 (---i+++)*",
			new String[] { "33bis", "1940-1945", "1940", "1945", "i" });

	}

}
