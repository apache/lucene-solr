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
package org.apache.lucene.analysis.ko;


import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.FilteringTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ko.tokenattributes.PartOfSpeechAttribute;

/**
 * Removes tokens that match a set of part-of-speech tags.
 * @lucene.experimental
 */
public final class KoreanPartOfSpeechStopFilter extends FilteringTokenFilter {
  private final Set<POS.Tag> stopTags;
  private final PartOfSpeechAttribute posAtt = addAttribute(PartOfSpeechAttribute.class);

  /**
   * Default list of tags to filter.
   */
  public static final Set<POS.Tag> DEFAULT_STOP_TAGS = new HashSet<>(Arrays.asList(
      POS.Tag.E,
      POS.Tag.IC,
      POS.Tag.J,
      POS.Tag.MAG,
      POS.Tag.MAJ,
      POS.Tag.MM,
      POS.Tag.SP,
      POS.Tag.SSC,
      POS.Tag.SSO,
      POS.Tag.SC,
      POS.Tag.SE,
      POS.Tag.XPN,
      POS.Tag.XSA,
      POS.Tag.XSN,
      POS.Tag.XSV,
      POS.Tag.UNA,
      POS.Tag.NA,
      POS.Tag.VSV
  ));

  /**
   * Create a new {@link KoreanPartOfSpeechStopFilter} with the default
   * list of stop tags {@link #DEFAULT_STOP_TAGS}.
   *
   * @param input    the {@link TokenStream} to consume
   */
  public KoreanPartOfSpeechStopFilter(TokenStream input) {
    this(input, DEFAULT_STOP_TAGS);
  }

  /**
   * Create a new {@link KoreanPartOfSpeechStopFilter}.
   * @param input    the {@link TokenStream} to consume
   * @param stopTags the part-of-speech tags that should be removed
   */
  public KoreanPartOfSpeechStopFilter(TokenStream input, Set<POS.Tag> stopTags) {
    super(input);
    this.stopTags = stopTags;
  }

  @Override
  protected boolean accept() {
    final POS.Tag leftPOS = posAtt.getLeftPOS();
    return leftPOS == null || !stopTags.contains(leftPOS);
  }
}
