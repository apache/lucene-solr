package org.apache.lucene.analysis.ja;

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

import java.util.Set;

import org.apache.lucene.analysis.ja.tokenattributes.PartOfSpeechAttribute;
import org.apache.lucene.analysis.util.FilteringTokenFilter;
import org.apache.lucene.analysis.TokenStream;

/**
 * Removes tokens that match a set of part-of-speech tags.
 */
public final class JapanesePartOfSpeechStopFilter extends FilteringTokenFilter {
  private final Set<String> stopTags;
  private final PartOfSpeechAttribute posAtt = addAttribute(PartOfSpeechAttribute.class);

  /**
   * Create a new {@link JapanesePartOfSpeechStopFilter}.
   * @param input    the {@link TokenStream} to consume
   * @param stopTags the part-of-speech tags that should be removed
   */
  public JapanesePartOfSpeechStopFilter(TokenStream input, Set<String> stopTags) {
    super(input);
    this.stopTags = stopTags;
  }

  @Override
  protected boolean accept() {
    final String pos = posAtt.getPartOfSpeech();
    return pos == null || !stopTags.contains(pos);
  }
}
