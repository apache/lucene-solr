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
package org.apache.lucene.analysis.miscellaneous;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.FilteringTokenFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharArraySet;

/**
 * A TokenFilter that only keeps tokens with text contained in the
 * required words.  This filter behaves like the inverse of StopFilter.
 * 
 * @since solr 1.3
 */
public final class KeepWordFilter extends FilteringTokenFilter {
  private final CharArraySet words;
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

  /**
   * Create a new {@link KeepWordFilter}.
   * <p><b>NOTE</b>: The words set passed to this constructor will be directly
   * used by this filter and should not be modified.
   * @param in      the {@link TokenStream} to consume
   * @param words   the words to keep
   */
  public KeepWordFilter(TokenStream in, CharArraySet words) {
    super(in);
    this.words = words;
  }

  @Override
  public boolean accept() {
    return words.contains(termAtt.buffer(), 0, termAtt.length());
  }
}
