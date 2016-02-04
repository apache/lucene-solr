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
package org.apache.lucene.analysis.core;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.analysis.util.FilteringTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.Lucene43FilteringTokenFilter;
import org.apache.lucene.util.Version;

/**
 * Backcompat StopFilter for versions 4.3 and before.
 * @deprecated Use {@link org.apache.lucene.analysis.core.StopFilter}
 */
@Deprecated
public final class Lucene43StopFilter extends Lucene43FilteringTokenFilter {

  private final CharArraySet stopWords;
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  
  public Lucene43StopFilter(boolean enablePositionIncrements, TokenStream in, CharArraySet stopWords) {
    super(enablePositionIncrements, in);
    this.stopWords = stopWords;
  }
  
  /**
   * Returns the next input Token whose term() is not a stop word.
   */
  @Override
  protected boolean accept() throws IOException {
    return !stopWords.contains(termAtt.buffer(), 0, termAtt.length());
  }

}
