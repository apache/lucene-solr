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


import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.util.Version;

import java.util.Map;
import java.io.IOException;

/**
 * Factory for {@link KeepWordFilter}. 
 * <pre class="prettyprint">
 * &lt;fieldType name="text_keepword" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *     &lt;filter class="solr.KeepWordFilterFactory" words="keepwords.txt" ignoreCase="false"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 */
public class KeepWordFilterFactory extends TokenFilterFactory implements ResourceLoaderAware {
  private final boolean ignoreCase;
  private final String wordFiles;
  private CharArraySet words;
  private boolean enablePositionIncrements;
  
  /** Creates a new KeepWordFilterFactory */
  public KeepWordFilterFactory(Map<String,String> args) {
    super(args);
    wordFiles = get(args, "words");
    ignoreCase = getBoolean(args, "ignoreCase", false);
    
    if (luceneMatchVersion.onOrAfter(Version.LUCENE_5_0_0) == false) {
      boolean defaultValue = luceneMatchVersion.onOrAfter(Version.LUCENE_4_4_0);
      enablePositionIncrements = getBoolean(args, "enablePositionIncrements", defaultValue);
      if (enablePositionIncrements == false && luceneMatchVersion.onOrAfter(Version.LUCENE_4_4_0)) {
        throw new IllegalArgumentException("enablePositionIncrements=false is not supported anymore as of Lucene 4.4");
      }
    } else if (args.containsKey("enablePositionIncrements")) {
      throw new IllegalArgumentException("enablePositionIncrements is not a valid option as of Lucene 5.0");
    }
    
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  @Override
  public void inform(ResourceLoader loader) throws IOException {
    if (wordFiles != null) {
      words = getWordSet(loader, wordFiles, ignoreCase);
    }
  }

  public boolean isIgnoreCase() {
    return ignoreCase;
  }

  public CharArraySet getWords() {
    return words;
  }

  @Override
  public TokenStream create(TokenStream input) {
    // if the set is null, it means it was empty
    if (words == null) {
      return input;
    } else if (luceneMatchVersion.onOrAfter(Version.LUCENE_4_4_0)) {
      return new KeepWordFilter(input, words);
    } else {
      @SuppressWarnings("deprecation")
      final TokenStream filter = new Lucene43KeepWordFilter(enablePositionIncrements, input, words);
      return filter;
    }
  }
}
