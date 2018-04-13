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

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.TokenFilterFactory;

/**
 * Factory for {@link KoreanPartOfSpeechStopFilter}.
 * @lucene.experimental
 */
public class KoreanPartOfSpeechStopFilterFactory extends TokenFilterFactory {
  private Set<POS.Tag> stopTags;

  /** Creates a new JapanesePartOfSpeechStopFilterFactory */
  public KoreanPartOfSpeechStopFilterFactory(Map<String,String> args) {
    super(args);
    Set<String> stopTagStr = getSet(args, "tags");
    if (stopTagStr == null) {
      stopTags = KoreanPartOfSpeechStopFilter.DEFAULT_STOP_TAGS;
    } else {
      stopTags = stopTagStr.stream().map(POS::resolveTag).collect(Collectors.toSet());
    }
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }
  
  @Override
  public TokenStream create(TokenStream stream) {
      return new KoreanPartOfSpeechStopFilter(stream, stopTags);
  }
}
