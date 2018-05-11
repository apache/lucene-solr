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

package org.apache.lucene.analysis.miscellaneous;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.ResourceLoader;

/**
 * Factory for a {@link TermExclusionFilter}
 */
public class TermExclusionFilterFactory extends ConditionalTokenFilterFactory {

  public static final String EXCLUDED_TOKENS = "protected";

  private final String wordFiles;
  private final boolean ignoreCase;

  private CharArraySet excludeTerms;

  public TermExclusionFilterFactory(Map<String, String> args) {
    super(args);
    wordFiles = get(args, EXCLUDED_TOKENS);
    ignoreCase = getBoolean(args, "ignoreCase", false);
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  @Override
  protected ConditionalTokenFilter create(TokenStream input, Function<TokenStream, TokenStream> inner) {
    return new TermExclusionFilter(excludeTerms, input, inner);
  }

  @Override
  public void doInform(ResourceLoader loader) throws IOException {
    excludeTerms = getWordSet(loader, wordFiles, ignoreCase);
  }
}
