package org.apache.lucene.analysis.core;

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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.analysis.util.TokenFilterFactory;

import java.util.Map;
import java.io.IOException;

/**
 * Factory for {@link StopFilter}.
 * <pre class="prettyprint">
 * &lt;fieldType name="text_stop" class="solr.TextField" positionIncrementGap="100" autoGeneratePhraseQueries="true"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *     &lt;filter class="solr.StopFilterFactory" ignoreCase="true"
 *             words="stopwords.txt" enablePositionIncrements="true"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 */
public class StopFilterFactory extends TokenFilterFactory implements ResourceLoaderAware {
  private CharArraySet stopWords;
  private final String stopWordFiles;
  private final String format;
  private final boolean ignoreCase;
  private final boolean enablePositionIncrements;
  
  /** Creates a new StopFilterFactory */
  public StopFilterFactory(Map<String,String> args) {
    super(args);
    assureMatchVersion();
    stopWordFiles = get(args, "words");
    format = get(args, "format");
    ignoreCase = getBoolean(args, "ignoreCase", false);
    enablePositionIncrements = getBoolean(args, "enablePositionIncrements", false);
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  @Override
  public void inform(ResourceLoader loader) throws IOException {
    if (stopWordFiles != null) {
      if ("snowball".equalsIgnoreCase(format)) {
        stopWords = getSnowballWordSet(loader, stopWordFiles, ignoreCase);
      } else {
        stopWords = getWordSet(loader, stopWordFiles, ignoreCase);
      }
    } else {
      stopWords = new CharArraySet(luceneMatchVersion, StopAnalyzer.ENGLISH_STOP_WORDS_SET, ignoreCase);
    }
  }

  public boolean isEnablePositionIncrements() {
    return enablePositionIncrements;
  }

  public boolean isIgnoreCase() {
    return ignoreCase;
  }

  public CharArraySet getStopWords() {
    return stopWords;
  }

  @Override
  public TokenStream create(TokenStream input) {
    StopFilter stopFilter = new StopFilter(luceneMatchVersion,input,stopWords);
    stopFilter.setEnablePositionIncrements(enablePositionIncrements);
    return stopFilter;
  }
}
