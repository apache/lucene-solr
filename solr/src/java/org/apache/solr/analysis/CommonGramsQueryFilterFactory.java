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
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.commongrams.CommonGramsFilter;
import org.apache.lucene.analysis.commongrams.CommonGramsQueryFilter;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.solr.common.ResourceLoader;
import org.apache.solr.util.plugin.ResourceLoaderAware;

/**
 * Construct {@link CommonGramsQueryFilter}.
 * 
 * This is pretty close to a straight copy from {@link StopFilterFactory}.
 * 
 * <pre class="prettyprint" >
 * &lt;fieldType name="text_cmmngrmsqry" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *     &lt;filter class="solr.CommonGramsQueryFilterFactory" words="commongramsquerystopwords.txt" ignoreCase="false"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 *
 */
public class CommonGramsQueryFilterFactory extends BaseTokenFilterFactory
    implements ResourceLoaderAware {

  @Override
  public void init(Map<String,String> args) {
    super.init(args);
    assureMatchVersion();
  }

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

  // Force the use of a char array set, as it is the most performant, although
  // this may break things if Lucene ever goes away from it. See SOLR-1095
  private CharArraySet commonWords;

  private boolean ignoreCase;

  public boolean isIgnoreCase() {
    return ignoreCase;
  }

  public Set<?> getCommonWords() {
    return commonWords;
  }

  /**
   * Create a CommonGramsFilter and wrap it with a CommonGramsQueryFilter
   */
  public CommonGramsQueryFilter create(TokenStream input) {
    CommonGramsFilter commonGrams = new CommonGramsFilter(luceneMatchVersion, input, commonWords,
        ignoreCase);
    CommonGramsQueryFilter commonGramsQuery = new CommonGramsQueryFilter(
        commonGrams);
    return commonGramsQuery;
  }
}
