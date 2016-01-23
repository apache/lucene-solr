package org.apache.lucene.analysis.miscellaneous;

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

import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.analysis.util.TokenFilterFactory;

/**
 * Factory for {@link KeywordMarkerFilter}.
 * <pre class="prettyprint">
 * &lt;fieldType name="text_keyword" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *     &lt;filter class="solr.KeywordMarkerFilterFactory" protected="protectedkeyword.txt" pattern="^.+er$" ignoreCase="false"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 */
public class KeywordMarkerFilterFactory extends TokenFilterFactory implements ResourceLoaderAware {
  public static final String PROTECTED_TOKENS = "protected";
  public static final String PATTERN = "pattern";
  private final String wordFiles;
  private final String stringPattern;
  private final boolean ignoreCase;
  private Pattern pattern;
  private CharArraySet protectedWords;
  
  /** Creates a new KeywordMarkerFilterFactory */
  public KeywordMarkerFilterFactory(Map<String,String> args) {
    super(args);
    wordFiles = get(args, PROTECTED_TOKENS);
    stringPattern = get(args, PATTERN);
    ignoreCase = getBoolean(args, "ignoreCase", false);
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }
  
  @Override
  public void inform(ResourceLoader loader) throws IOException {
    if (wordFiles != null) {  
      protectedWords = getWordSet(loader, wordFiles, ignoreCase);
    }
    if (stringPattern != null) {
      pattern = ignoreCase ? Pattern.compile(stringPattern, Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE) : Pattern.compile(stringPattern);
    }
  }
  
  public boolean isIgnoreCase() {
    return ignoreCase;
  }

  @Override
  public TokenStream create(TokenStream input) {
    if (pattern != null) {
      input = new PatternKeywordMarkerFilter(input, pattern);
    }
    if (protectedWords != null) {
      input = new SetKeywordMarkerFilter(input, protectedWords);
    }
    return input;
  }
}
