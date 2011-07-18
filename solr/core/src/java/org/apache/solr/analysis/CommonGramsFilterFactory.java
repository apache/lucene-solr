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
package org.apache.solr.analysis;

import java.io.IOException;
import java.util.Set;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.commongrams.CommonGramsFilter;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.solr.common.ResourceLoader;
import org.apache.solr.util.plugin.ResourceLoaderAware;

/**
 * Constructs a {@link CommonGramsFilter}.
 * <pre class="prettyprint" >
 * &lt;fieldType name="text_cmmngrms" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *     &lt;filter class="solr.CommonGramsFilterFactory" words="commongramsstopwords.txt" ignoreCase="false"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 *
 */

/*
 * This is pretty close to a straight copy from StopFilterFactory
 */
public class CommonGramsFilterFactory extends BaseTokenFilterFactory implements
    ResourceLoaderAware {

  public void inform(ResourceLoader loader) {
    String commonWordFiles = args.get("words");
    ignoreCase = getBoolean("ignoreCase", false);

    if (commonWordFiles != null) {
      try {
        commonWords = getWordSet(loader, commonWordFiles, ignoreCase);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      commonWords = (CharArraySet) StopAnalyzer.ENGLISH_STOP_WORDS_SET;
    }
  }
      
    //Force the use of a char array set, as it is the most performant, although this may break things if Lucene ever goes away from it.  See SOLR-1095
    private CharArraySet commonWords;
    private boolean ignoreCase;

  public boolean isIgnoreCase() {
    return ignoreCase;
  }

  public Set<?> getCommonWords() {
    return commonWords;
  }

  public CommonGramsFilter create(TokenStream input) {
    CommonGramsFilter commonGrams = new CommonGramsFilter(luceneMatchVersion, input, commonWords, ignoreCase);
    return commonGrams;
  }
}
 
  
  