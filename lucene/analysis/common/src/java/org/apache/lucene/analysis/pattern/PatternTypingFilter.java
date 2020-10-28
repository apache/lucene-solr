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
import java.util.regex.Matcher;
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

  private final PatternTypingRule[] replacementAndFlagByPattern;
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final FlagsAttribute flagAtt = addAttribute(FlagsAttribute.class);
  private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);

  public PatternTypingFilter(TokenStream input,  PatternTypingRule... replacementAndFlagByPattern) {
    super(input);
    this.replacementAndFlagByPattern = replacementAndFlagByPattern;
  }

  @Override
  public final boolean incrementToken() throws IOException {
    if (input.incrementToken()) {
      for (PatternTypingRule rule : replacementAndFlagByPattern) {
        Matcher matcher = rule.getPattern().matcher(termAtt);
        if (matcher.find()) {
          // allow 2nd reset() and find() that occurs inside replaceFirst to avoid excess string creation
          typeAtt.setType(matcher.replaceFirst(rule.getTypeTemplate()));
          flagAtt.setFlags(rule.getFlags());
          return true;
        }
      }
      return true;
    }
    return false;
  }

  /**
   * Value holding class for pattern typing rules.
   */
  public static class PatternTypingRule {
    private final Pattern pattern;
    private final int flags;
    private final String typeTemplate;

    public PatternTypingRule(Pattern pattern, int flags, String typeTemplate) {
      this.pattern = pattern;
      this.flags = flags;
      this.typeTemplate = typeTemplate;
    }

    public Pattern getPattern() {
      return pattern;
    }

    public int getFlags() {
      return flags;
    }

    public String getTypeTemplate() {
      return typeTemplate;
    }
  }
}
