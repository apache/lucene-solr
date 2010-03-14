package org.apache.solr.analysis;

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

import java.io.IOException;

import org.apache.lucene.analysis.*;

/**
 * When the plain text is extracted from documents, we will often have many words hyphenated and broken into
 * two lines. This is often the case with documents where narrow text columns are used, such as newsletters.
 * In order to increase search efficiency, this filter puts hyphenated words broken into two lines back together.
 * This filter should be used on indexing time only.
 * Example field definition in schema.xml:
 * <pre>
 * &lt;fieldtype name="text" class="solr.TextField" positionIncrementGap="100"&gt;
 * 	&lt;analyzer type="index"&gt;
 * 		&lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *      &lt;filter class="solr.SynonymFilterFactory" synonyms="index_synonyms.txt" ignoreCase="true" expand="false"/&gt;
 *      &lt;filter class="solr.StopFilterFactory" ignoreCase="true"/&gt;
 *      &lt;filter class="solr.HyphenatedWordsFilterFactory"/&gt;
 *      &lt;filter class="solr.WordDelimiterFilterFactory" generateWordParts="1" generateNumberParts="1" catenateWords="1" catenateNumbers="1" catenateAll="0"/&gt;
 *      &lt;filter class="solr.LowerCaseFilterFactory"/&gt;
 *      &lt;filter class="solr.RemoveDuplicatesTokenFilterFactory"/&gt;
 *  &lt;/analyzer&gt;
 *  &lt;analyzer type="query"&gt;
 *      &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *      &lt;filter class="solr.SynonymFilterFactory" synonyms="synonyms.txt" ignoreCase="true" expand="true"/&gt;
 *      &lt;filter class="solr.StopFilterFactory" ignoreCase="true"/&gt;
 *      &lt;filter class="solr.WordDelimiterFilterFactory" generateWordParts="1" generateNumberParts="1" catenateWords="0" catenateNumbers="0" catenateAll="0"/&gt;
 *      &lt;filter class="solr.LowerCaseFilterFactory"/&gt;
 *      &lt;filter class="solr.RemoveDuplicatesTokenFilterFactory"/&gt;
 *  &lt;/analyzer&gt;
 * &lt;/fieldtype&gt;
 * </pre>
 * 
 */
public final class HyphenatedWordsFilter extends TokenFilter {

	public HyphenatedWordsFilter(TokenStream in) {
		super(in);
	}



  /**
	 * @inheritDoc
	 * @see org.apache.lucene.analysis.TokenStream#next()
	 */
	public final Token next(Token in) throws IOException {
		StringBuilder termText = new StringBuilder(25);
		int startOffset = -1, firstPositionIncrement = -1, wordsMerged = 0;
		Token lastToken = null;
		for (Token token = input.next(in); token != null; token = input.next()) {
			termText.append(token.termBuffer(), 0, token.termLength());
			//current token ends with hyphen -> grab the next token and glue them together
			if (termText.charAt(termText.length() - 1) == '-') {
				wordsMerged++;
				//remove the hyphen
				termText.setLength(termText.length()-1);
				if (startOffset == -1) {
					startOffset = token.startOffset();
					firstPositionIncrement = token.getPositionIncrement();
				}
				lastToken = token;
			} else {
				//shortcut returns token
				if (wordsMerged == 0)
					return token;
				Token mergedToken = new Token(termText.toString(), startOffset, token.endOffset(), token.type());
				mergedToken.setPositionIncrement(firstPositionIncrement);
				return mergedToken;
			}
		}
		//last token ending with hyphen? - we know that we have only one token in
		//this situation, so we can safely return firstToken
		if (startOffset != -1)
			return lastToken;
		else
			return null; //end of token stream
	}
}
