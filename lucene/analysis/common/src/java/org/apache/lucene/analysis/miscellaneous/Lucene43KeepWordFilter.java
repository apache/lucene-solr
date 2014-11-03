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
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.Lucene43FilteringTokenFilter;

/**
 * Backcompat for KeepWordFilter for versions 4.3 and before.
 * @deprecated Use {@link org.apache.lucene.analysis.miscellaneous.KeepWordFilter}
 */
@Deprecated
public final class Lucene43KeepWordFilter extends Lucene43FilteringTokenFilter {
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final CharArraySet words;

  /** The words set passed to this constructor will be directly used by this filter
   * and should not be modified, */
  public Lucene43KeepWordFilter(boolean enablePositionIncrements, TokenStream in, CharArraySet words) {
    super(enablePositionIncrements, in);
    this.words = words;
  }

  public boolean accept() {
    return words.contains(termAtt.buffer(), 0, termAtt.length());
  }
}
