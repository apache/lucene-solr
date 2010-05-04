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
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

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

  private final CharTermAttribute termAttribute = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAttribute = addAttribute(OffsetAttribute.class);
  
  private final StringBuilder hyphenated = new StringBuilder();
  private State savedState;

  /**
   * Creates a new HyphenatedWordsFilter
   *
   * @param in TokenStream that will be filtered
   */
  public HyphenatedWordsFilter(TokenStream in) {
    super(in);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean incrementToken() throws IOException {
    while (input.incrementToken()) {
      char[] term = termAttribute.buffer();
      int termLength = termAttribute.length();
      
      if (termLength > 0 && term[termLength - 1] == '-') {
        // a hyphenated word
        // capture the state of the first token only
        if (savedState == null) {
          savedState = captureState();
        }
        hyphenated.append(term, 0, termLength - 1);
      } else if (savedState == null) {
        // not part of a hyphenated word.
        return true;
      } else {
        // the final portion of a hyphenated word
        hyphenated.append(term, 0, termLength);
        unhyphenate();
        return true;
      }
    }
    
    if (savedState != null) {
      // the final term ends with a hyphen
      // add back the hyphen, for backwards compatibility.
      hyphenated.append('-');
      unhyphenate();
      return true;
    }
    
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void reset() throws IOException {
    super.reset();
    hyphenated.setLength(0);
    savedState = null;
  }

  // ================================================= Helper Methods ================================================

  /**
   * Writes the joined unhyphenated term
   */
  private void unhyphenate() {
    int endOffset = offsetAttribute.endOffset();
    
    restoreState(savedState);
    savedState = null;
    
    char term[] = termAttribute.buffer();
    int length = hyphenated.length();
    if (length > termAttribute.length()) {
      term = termAttribute.resizeBuffer(length);
    }
    
    hyphenated.getChars(0, length, term, 0);
    termAttribute.setLength(length);
    offsetAttribute.setOffset(offsetAttribute.startOffset(), endOffset);
    hyphenated.setLength(0);
  }
}
