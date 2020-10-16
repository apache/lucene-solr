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

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.FlagsAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;

/**
 * Set a type attribute to a parameterized value when tokens are matched by any of a several regex patterns. The
 * value set in the type attribute is parameterized with the match groups of the regex used for matching.
 * In combination with TypeAsSynonymFilter and DropIfFlagged filter this can supply complex synonym patterns
 * that are protected from subsequent analysis, and optionally drop the original term based on the flag
 * set in this filter. See {@link PatternTypingFilterFactory} for full documentation.
 *
 * @since 8.8.0
 * @see PatternTypingFilterFactory
 */
public class PatternTypingFilter extends TokenFilter {

  private final Map<Pattern, String> patterns;
  private final Map<Pattern, Integer> flags;
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final FlagsAttribute flagAtt = addAttribute(FlagsAttribute.class);
  private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);

  public PatternTypingFilter(TokenStream input, LinkedHashMap<Pattern,String> patterns, Map<Pattern,Integer> flags) {
    super(input);
    this.patterns = patterns;
    this.flags = flags;
  }

  @Override
  public final boolean incrementToken() throws IOException {
    if (input.incrementToken()) {
      if (hasAttribute(CharTermAttribute.class)) {
        String termText = termAtt.toString();
        for (Map.Entry<Pattern, String> patRep : patterns.entrySet()) {
          Pattern pattern = patRep.getKey();
          Matcher matcher = pattern.matcher(termText);
          String replaced = matcher.replaceFirst(patRep.getValue());
          // N.B. Does not support producing a synonym identical to the original term.
          // Avoids having to match() then replace() which performs a second find().
          if (!replaced.equals(termText)) {
            typeAtt.setType(replaced);
            flagAtt.setFlags(flags.get(pattern));
            return true;
          }
        }
      }
      return true;
    }
    return false;
  }
}
