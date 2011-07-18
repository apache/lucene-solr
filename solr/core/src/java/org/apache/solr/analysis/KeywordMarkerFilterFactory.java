package org.apache.solr.analysis;

import java.io.IOException;

import org.apache.lucene.analysis.miscellaneous.KeywordMarkerFilter;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.solr.common.ResourceLoader;
import org.apache.solr.util.plugin.ResourceLoaderAware;

/**
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

/**
 * Factory for {@link KeywordMarkerFilter}.
 * <pre class="prettyprint" >
 * &lt;fieldType name="text_keyword" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *     &lt;filter class="solr.KeywordMarkerFilterFactory" protected="protectedkeyword.txt" ignoreCase="false"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre> 
 *
 */
public class KeywordMarkerFilterFactory extends BaseTokenFilterFactory implements ResourceLoaderAware {
  public static final String PROTECTED_TOKENS = "protected";
  private CharArraySet protectedWords;
  private boolean ignoreCase;
  
  public void inform(ResourceLoader loader) {
    String wordFiles = args.get(PROTECTED_TOKENS);
    ignoreCase = getBoolean("ignoreCase", false);
    if (wordFiles != null) {  
      try {
        protectedWords = getWordSet(loader, wordFiles, ignoreCase);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
  
  public boolean isIgnoreCase() {
    return ignoreCase;
  }

  public TokenStream create(TokenStream input) {
    return protectedWords == null ? input : new KeywordMarkerFilter(input, protectedWords);
  }
}
