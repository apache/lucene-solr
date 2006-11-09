/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
