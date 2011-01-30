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

import org.apache.lucene.analysis.FilteringTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.IOException;
import java.util.Set;

/**
 * A TokenFilter that only keeps tokens with text contained in the
 * required words.  This filter behaves like the inverse of StopFilter.
 * 
 * @version $Id$
 * @since solr 1.3
 */
public final class KeepWordFilter extends FilteringTokenFilter {
  private final CharArraySet words;
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

  /** @deprecated Use {@link #KeepWordFilter(boolean, TokenStream, CharArraySet)} instead */
  @Deprecated
  public KeepWordFilter(TokenStream in, Set<String> words, boolean ignoreCase ) {
    this(false, in, new CharArraySet(words, ignoreCase));
  }

  /** The words set passed to this constructor will be directly used by this filter
   * and should not be modified, */
  public KeepWordFilter(boolean enablePositionIncrements, TokenStream in, CharArraySet words) {
    super(enablePositionIncrements, in);
    this.words = words;
  }

  @Override
  public boolean accept() throws IOException {
    return words.contains(termAtt.buffer(), 0, termAtt.length());
  }
}
