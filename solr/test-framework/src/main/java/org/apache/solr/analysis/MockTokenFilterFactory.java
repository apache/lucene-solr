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
package org.apache.solr.analysis;

import java.util.Arrays;
import java.util.Map;

import org.apache.lucene.analysis.MockTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.RegExp;

/**
 * Factory for {@link MockTokenFilter} for testing purposes.
 */
public class MockTokenFilterFactory extends TokenFilterFactory {
  final CharacterRunAutomaton filter;

  /** Creates a new MockTokenizerFactory */
  public MockTokenFilterFactory(Map<String, String> args) {
    super(args);
    String stopset = get(args, "stopset", Arrays.asList("english", "empty"), null, false);
    String stopregex = get(args, "stopregex");
    if (null != stopset) {
      if (null != stopregex) {
        throw new IllegalArgumentException("Parameters stopset and stopregex cannot both be specified.");
      }
      if ("english".equalsIgnoreCase(stopset)) {
        filter = MockTokenFilter.ENGLISH_STOPSET;
      } else { // must be "empty"
        filter = MockTokenFilter.EMPTY_STOPSET;
      }
    } else if (null != stopregex) {
      RegExp regex = new RegExp(stopregex);
      filter = new CharacterRunAutomaton(regex.toAutomaton());
    } else {
      throw new IllegalArgumentException
          ("Configuration Error: either the 'stopset' or the 'stopregex' parameter must be specified.");
    }
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  @Override
  public MockTokenFilter create(TokenStream stream) {
    return new MockTokenFilter(stream, filter);
  }
}
