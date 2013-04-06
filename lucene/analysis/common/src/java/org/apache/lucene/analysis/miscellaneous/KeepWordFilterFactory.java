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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.analysis.util.TokenFilterFactory;

import java.util.Map;
import java.io.IOException;

/**
 * Factory for {@link KeepWordFilter}. 
 * <pre class="prettyprint">
 * &lt;fieldType name="text_keepword" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *     &lt;filter class="solr.KeepWordFilterFactory" words="keepwords.txt" ignoreCase="false" enablePositionIncrements="false"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 */
public class KeepWordFilterFactory extends TokenFilterFactory implements ResourceLoaderAware {
  private final boolean ignoreCase;
  private final boolean enablePositionIncrements;
  private final String wordFiles;
  private CharArraySet words;
  
  /** Creates a new KeepWordFilterFactory */
  public KeepWordFilterFactory(Map<String,String> args) {
    super(args);
    assureMatchVersion();
    wordFiles = get(args, "words");
    ignoreCase = getBoolean(args, "ignoreCase", false);
    enablePositionIncrements = getBoolean(args, "enablePositionIncrements", false);
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

  public boolean isEnablePositionIncrements() {
    return enablePositionIncrements;
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
    return words == null ? input : new KeepWordFilter(enablePositionIncrements, input, words);
  }
}
