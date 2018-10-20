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
package org.apache.lucene.analysis.phonetic;


import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.language.bm.BeiderMorseEncoder; // javadocs
import org.apache.commons.codec.language.bm.Languages.LanguageSet;
import org.apache.commons.codec.language.bm.PhoneticEngine;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

/**
 * TokenFilter for Beider-Morse phonetic encoding.
 * @see BeiderMorseEncoder
 * @lucene.experimental
 */
public final class BeiderMorseFilter extends TokenFilter {
  private final PhoneticEngine engine;
  private final LanguageSet languages;
  
  // output is a string such as ab|ac|...
  // in complex cases like d'angelo it's (anZelo|andZelo|...)-(danZelo|...)
  // if there are multiple 's, it starts to nest...
  private static final Pattern pattern = Pattern.compile("([^()|-]+)");
  
  // matcher over any buffered output
  private final Matcher matcher = pattern.matcher("");
  // encoded representation
  private String encoded;
  // preserves all attributes for any buffered outputs
  private State state;
  
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);
  
  
  /**
   * Calls
   * {@link #BeiderMorseFilter(TokenStream, PhoneticEngine, org.apache.commons.codec.language.bm.Languages.LanguageSet)}
   * 
   * @param input TokenStream to filter
   * @param engine configured PhoneticEngine with BM settings.
   */
  public BeiderMorseFilter(TokenStream input, PhoneticEngine engine) {
    this(input, engine, null);
  }

  /**
   * Create a new BeiderMorseFilter
   * @param input TokenStream to filter
   * @param engine configured PhoneticEngine with BM settings.
   * @param languages optional Set of original languages. Can be null (which means it will be guessed).
   */
  public BeiderMorseFilter(TokenStream input, PhoneticEngine engine, LanguageSet languages) {
    super(input);
    this.engine = engine;
    this.languages = languages;
  }

  @Override
  public boolean incrementToken() throws IOException {
    if (matcher.find()) {
      assert state != null && encoded != null;
      restoreState(state);
      termAtt.setEmpty().append(encoded, matcher.start(1), matcher.end(1));
      posIncAtt.setPositionIncrement(0);
      return true;
    }
    
    if (input.incrementToken()) {
      encoded = (languages == null) 
          ? engine.encode(termAtt.toString())
          : engine.encode(termAtt.toString(), languages);
      state = captureState();
      matcher.reset(encoded);
      if (matcher.find()) {
        termAtt.setEmpty().append(encoded, matcher.start(1), matcher.end(1));
      }
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    matcher.reset("");
  }
}
