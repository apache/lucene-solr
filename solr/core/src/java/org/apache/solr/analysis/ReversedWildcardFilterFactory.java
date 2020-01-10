/*
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
import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.reverse.ReverseStringFilter;
import org.apache.lucene.analysis.util.TokenFilterFactory;

/**
 * Factory for {@link ReversedWildcardFilter}-s. When this factory is
 * added to an analysis chain, it will be used both for filtering the
 * tokens during indexing, and to determine the query processing of
 * this field during search.
 * <p>This class supports the following init arguments:
 * <ul>
 * <li><code>withOriginal</code> - if true, then produce both original and reversed tokens at
 * the same positions. If false, then produce only reversed tokens.</li>
 * <li><code>maxPosAsterisk</code> - maximum position (1-based) of the asterisk wildcard
 * ('*') that triggers the reversal of query term. Asterisk that occurs at
 * positions higher than this value will not cause the reversal of query term.
 * Defaults to 2, meaning that asterisks on positions 1 and 2 will cause
 * a reversal.</li>
 * <li><code>maxPosQuestion</code> - maximum position (1-based) of the question
 * mark wildcard ('?') that triggers the reversal of query term. Defaults to 1.
 * Set this to 0, and <code>maxPosAsterisk</code> to 1 to reverse only
 * pure suffix queries (i.e. ones with a single leading asterisk).</li>
 * <li><code>maxFractionAsterisk</code> - additional parameter that
 * triggers the reversal if asterisk ('*') position is less than this
 * fraction of the query token length. Defaults to 0.0f (disabled).</li>
 * <li><code>minTrailing</code> - minimum number of trailing characters in query
 * token after the last wildcard character. For good performance this should be
 * set to a value larger than 1. Defaults to 2.
 * </ul>
 * Note 1: This filter always reverses input tokens during indexing.
 * Note 2: Query tokens without wildcard characters will never be reversed.
 * <pre class="prettyprint" >
 * &lt;fieldType name="text_rvswc" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer type="index"&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *     &lt;filter class="solr.ReversedWildcardFilterFactory" withOriginal="true"
 *             maxPosAsterisk="2" maxPosQuestion="1" minTrailing="2" maxFractionAsterisk="0"/&gt;
 *   &lt;/analyzer&gt;
 *   &lt;analyzer type="query"&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 *
 * @since 3.1
 * @lucene.spi {@value #NAME}
 */
public class ReversedWildcardFilterFactory extends TokenFilterFactory {

  /** SPI name */
  public static final String NAME = "reversedWildcard";
  
  private char markerChar = ReverseStringFilter.START_OF_HEADING_MARKER;
  private boolean withOriginal;
  private int maxPosAsterisk;
  private int maxPosQuestion;
  private int minTrailing;
  private float maxFractionAsterisk;

  /** Creates a new ReversedWildcardFilterFactory */
  public ReversedWildcardFilterFactory(Map<String,String> args) {
    super(args);
    withOriginal = getBoolean(args, "withOriginal", true);
    maxPosAsterisk = getInt(args, "maxPosAsterisk", 2);
    maxPosQuestion = getInt(args, "maxPosQuestion", 1);
    minTrailing = getInt(args, "minTrailing", 2);
    maxFractionAsterisk = getFloat(args, "maxFractionAsterisk", 0.0f);
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }


  @Override
  public TokenStream create(TokenStream input) {
    return new ReversedWildcardFilter(input, withOriginal, markerChar);
  }
  
  /**
   * This method encapsulates the logic that determines whether
   * a query token should be reversed in order to use the
   * reversed terms in the index.
   * @param token input token.
   * @return true if input token should be reversed, false otherwise.
   */
  public boolean shouldReverse(String token) {
    int posQ = token.indexOf('?');
    int posA = token.indexOf('*');
    if (posQ == -1 && posA == -1) { // not a wildcard query
      return false;
    }
    int pos;
    int lastPos;
    int len = token.length();
    lastPos = token.lastIndexOf('?');
    pos = token.lastIndexOf('*');
    if (pos > lastPos) lastPos = pos;
    if (posQ != -1) {
      pos = posQ;
      if (posA != -1) {
        pos = Math.min(posQ, posA);
      }
    } else {
      pos = posA;
    }
    if (len - lastPos < minTrailing)  { // too few trailing chars
      return false;
    }
    if (posQ != -1 && posQ < maxPosQuestion) {  // leading '?'
      return true;
    }
    if (posA != -1 && posA < maxPosAsterisk) { // leading '*'
      return true;
    }
    // '*' in the leading part
    if (maxFractionAsterisk > 0.0f && pos < (float)token.length() * maxFractionAsterisk) {
      return true;
    }
    return false;
  }
  
  public char getMarkerChar() {
    return markerChar;
  }
}
