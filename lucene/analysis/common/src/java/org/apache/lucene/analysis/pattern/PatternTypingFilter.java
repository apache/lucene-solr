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

package org.apache.lucene.analysis.pattern;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.FlagsAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Set a type attribute to a parameterized value when tokens are matched by any of a several regex patterns. The
 * value set in the type attribute is parameterized with the match groups of the regex used for matching.
 * In combination with TypeAsSynonymFilter and DropIfFlagged filter this can supply complex synonym patterns
 * that are protected from subsequent analysis, and optionally drop the original term based on the flag
 * set in this filter. See {@link PatternTypingFilterFactory} for full documentation.
 *
 * @see PatternTypingFilterFactory
 * @since 8.8.0
 */
public class PatternTypingFilter extends TokenFilter {

  private final Map<Pattern, Map.Entry<String, Integer>> replacementAndFlagByPattern;
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final FlagsAttribute flagAtt = addAttribute(FlagsAttribute.class);
  private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);

  public PatternTypingFilter(TokenStream input, LinkedHashMap<Pattern, Map.Entry<String, Integer>> replacementAndFlagByPattern) {
    super(input);
    this.replacementAndFlagByPattern = replacementAndFlagByPattern;
  }

  @Override
  public final boolean incrementToken() throws IOException {
    if (input.incrementToken()) {
      for (Map.Entry<Pattern, Map.Entry<String, Integer>> patRep : replacementAndFlagByPattern.entrySet()) {
        if (patRep.getKey().matcher(termAtt).find()) {
          Map.Entry<String, Integer> replAndFlags = patRep.getValue();
          // allow 2nd reset() and find() that occurs inside replaceFirst to avoid excess string creation
          typeAtt.setType(patRep.getKey().matcher(termAtt).replaceFirst(replAndFlags.getKey()));
          flagAtt.setFlags(replAndFlags.getValue());
          return true;
        }
      }
      return true;
    }
    return false;
  }
}
