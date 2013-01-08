package org.apache.lucene.analysis.compound;

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

import org.apache.lucene.analysis.util.*;
import org.apache.lucene.analysis.TokenStream;

import java.util.Map;
import java.io.IOException;

/** 
 * Factory for {@link DictionaryCompoundWordTokenFilter}. 
 * <pre class="prettyprint" >
 * &lt;fieldType name="text_dictcomp" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *     &lt;filter class="solr.DictionaryCompoundWordTokenFilterFactory" dictionary="dictionary.txt"
 *         minWordSize="5" minSubwordSize="2" maxSubwordSize="15" onlyLongestMatch="true"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 *
 */
public class DictionaryCompoundWordTokenFilterFactory extends TokenFilterFactory  implements ResourceLoaderAware {
  private CharArraySet dictionary;
  private String dictFile;
  private int minWordSize;
  private int minSubwordSize;
  private int maxSubwordSize;
  private boolean onlyLongestMatch;
  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    assureMatchVersion();
    dictFile = args.get("dictionary");
    if (null == dictFile) {
      throw new IllegalArgumentException("Missing required parameter: dictionary");
    }

    minWordSize= getInt("minWordSize",CompoundWordTokenFilterBase.DEFAULT_MIN_WORD_SIZE);
    minSubwordSize= getInt("minSubwordSize",CompoundWordTokenFilterBase.DEFAULT_MIN_SUBWORD_SIZE);
    maxSubwordSize= getInt("maxSubwordSize",CompoundWordTokenFilterBase.DEFAULT_MAX_SUBWORD_SIZE);
    onlyLongestMatch = getBoolean("onlyLongestMatch",true);
  }
  @Override
  public void inform(ResourceLoader loader) throws IOException {
    dictionary = super.getWordSet(loader, dictFile, false);
  }
  @Override
  public TokenStream create(TokenStream input) {
    // if the dictionary is null, it means it was empty
    return dictionary == null ? input : new DictionaryCompoundWordTokenFilter(luceneMatchVersion,input,dictionary,minWordSize,minSubwordSize,maxSubwordSize,onlyLongestMatch);
  }
}

