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

import java.io.IOException;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.Lucene43FilteringTokenFilter;

/**
 * Backcompat LengthFilter for versions 4.3 and before.
 * @deprecated Use {@link org.apache.lucene.analysis.miscellaneous.LengthFilter}
 */
@Deprecated
public final class Lucene43LengthFilter extends Lucene43FilteringTokenFilter {

  private final int min;
  private final int max;
  
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

  /**
   * Build a filter that removes words that are too long or too
   * short from the text.
   */
  public Lucene43LengthFilter(boolean enablePositionIncrements, TokenStream in, int min, int max) {
    super(enablePositionIncrements, in);
    this.min = min;
    this.max = max;
  }
  
  @Override
  public boolean accept() throws IOException {
    final int len = termAtt.length();
    return (len >= min && len <= max);
  }
}
