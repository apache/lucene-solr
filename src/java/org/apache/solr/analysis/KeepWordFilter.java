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

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.tartarus.snowball.SnowballProgram;

import java.io.IOException;
import java.util.Set;

/**
 * A TokenFilter that only keeps tokens with text contained in the
 * required words.  This filter behaves like the inverse of StopFilter.
 * 
 * @version $Id$
 * @since solr 1.3
 */
public final class KeepWordFilter extends TokenFilter {
  private final CharArraySet words;
  private final TermAttribute termAtt;

  public KeepWordFilter(TokenStream in, Set<String> words, boolean ignoreCase ) {
    super(in);
    this.words = new CharArraySet(words, ignoreCase);
    this.termAtt = (TermAttribute)addAttribute(TermAttribute.class);
  }

  @Override
  public boolean incrementToken() throws IOException {
    while (input.incrementToken()) {
      if (words.contains(termAtt.termBuffer(), 0, termAtt.termLength())) return true;
    }
    return false;
  }
}
