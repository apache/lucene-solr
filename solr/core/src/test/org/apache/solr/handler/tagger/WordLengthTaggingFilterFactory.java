/*
 * This software was produced for the U. S. Government
 * under Contract No. W15P7T-11-C-F600, and is
 * subject to the Rights in Noncommercial Computer Software
 * and Noncommercial Computer Software Documentation
 * Clause 252.227-7014 (JUN 1995)
 *
 * Copyright 2013 The MITRE Corporation. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.handler.tagger;

import java.lang.invoke.MethodHandles;
import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @lucene.spi {@value #NAME}
 */
public class WordLengthTaggingFilterFactory extends TokenFilterFactory {

  /** SPI name */
  public static final String NAME = "wordLengthTagging";

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String MIN_LENGTH = "minLength";

  private final Integer minLength;

  public WordLengthTaggingFilterFactory(Map<String, String> args) {
    super(args);
    int minLength = -1;
    Object value = args.get(MIN_LENGTH);
    if (value != null) {
      try {
        minLength = Integer.parseInt(value.toString());
      } catch (NumberFormatException e) {
        log.warn("Unable to parse minLength from value 'minLength=\"{}\"'", value);

      }
    }
    if (minLength <= 0) {
      log.info("use default minLength={}", WordLengthTaggingFilter.DEFAULT_MIN_LENGTH);
      this.minLength = null;
    } else {
      log.info("set minLength={}", minLength);
      this.minLength = minLength;
    }
  }

  @Override
  public TokenStream create(TokenStream input) {
    return new WordLengthTaggingFilter(input, minLength);
  }

}
