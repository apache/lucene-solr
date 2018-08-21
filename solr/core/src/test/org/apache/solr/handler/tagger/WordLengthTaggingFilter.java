/*
 * This software was produced for the U. S. Government
 * under Contract No. W15P7T-11-C-F600, and is
 * subject to the Rights in Noncommercial Computer Software
 * and Noncommercial Computer Software Documentation
 * Clause 252.227-7014 (JUN 1995)
 *
 * Copyright 2013 The MITRE Corporation. All Rights Reserved.
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

package org.apache.solr.handler.tagger;

import java.io.IOException;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;

/**
 * Simple TokenFilter that lookup only Tokens with more as the parsed number
 * of chars.<p>
 * <b>NOTE:</b>This implementation is only intended to be used as an example
 * and for unit testing the {@link TaggingAttribute} feature. Typically
 * implementations will be based on NLP results (e.g. using POS tags or
 * detected Named Entities).
 * <p>
 * <b>Example Usage:</b><p>
 * Currently the usage requires to modify the Analyzer as defined by the
 * <code>indexedField</code>. An alternative would be to allow the configuration
 * of a special FieldType in the schema.xml and use this Analyzer for processing
 * the text sent to the request.<p>
 * While the current solution is fine for direct API usage, defining the
 * Analyzer in the schema.xml would be better suitable for using this feature
 * with the {@link TaggerRequestHandler}.
 *
 * <pre class="prettyprint">
 *     Analyzer analyzer = req.getSchema().getField(indexedField).getType().getAnalyzer();
 *     //get the TokenStream from the Analyzer
 *     TokenStream baseStream = analyzer.tokenStream("", reader);
 *     //add a FilterStream that sets the LookupAttribute to the end
 *     TokenStream filterStream = new WordLengthLookupFilter(baseStream);
 *     //create the Tagger using the modified analyzer chain.
 *     new Tagger(corpus, filterStream, tagClusterReducer) {
 *
 *         protected void tagCallback(int startOffset, int endOffset, long docIdsKey) {
 *             //implement the callback
 *         }
 *
 *     }.process();
 * </pre>
 */
public class WordLengthTaggingFilter extends TokenFilter {

  /**
   * The default minimum length is <code>3</code>
   */
  public static final int DEFAULT_MIN_LENGTH = 3;
  private final TaggingAttribute lookupAtt = addAttribute(TaggingAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  private int minLength;

  /**
   * TokenFilter only marks tokens to be looked up with equals or more as
   * {@link #DEFAULT_MIN_LENGTH} characters
   */
  public WordLengthTaggingFilter(TokenStream input) {
    this(input, null);
  }

  /**
   * TokenFilter only marks tokens to be looked up with equals or more characters
   * as the parsed minimum.
   *
   * @param input     the TokenStream to consume tokens from
   * @param minLength The minimum length to lookup a Token. <code>null</code>
   *                  or &lt;= 0 to use the #DEFAULT_MIN_LENGTH
   */
  public WordLengthTaggingFilter(TokenStream input, Integer minLength) {
    super(input);
    if (minLength == null || minLength <= 0) {
      this.minLength = DEFAULT_MIN_LENGTH;
    } else {
      this.minLength = minLength;
    }
  }

  @Override
  public final boolean incrementToken() throws IOException {
    if (input.incrementToken()) {
      int size = offsetAtt.endOffset() - offsetAtt.startOffset();
      lookupAtt.setTaggable(size >= minLength);
      return true;
    } else {
      return false;
    }
  }

}
