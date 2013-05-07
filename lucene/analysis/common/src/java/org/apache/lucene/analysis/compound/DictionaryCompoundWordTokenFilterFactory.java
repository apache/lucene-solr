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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.analysis.util.TokenFilterFactory;

import java.util.Map;
import java.io.IOException;

/** 
 * Factory for {@link DictionaryCompoundWordTokenFilter}. 
 * <pre class="prettyprint">
 * &lt;fieldType name="text_dictcomp" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *     &lt;filter class="solr.DictionaryCompoundWordTokenFilterFactory" dictionary="dictionary.txt"
 *         minWordSize="5" minSubwordSize="2" maxSubwordSize="15" onlyLongestMatch="true"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 */
public class DictionaryCompoundWordTokenFilterFactory extends TokenFilterFactory implements ResourceLoaderAware {
  private CharArraySet dictionary;
  private final String dictFile;
  private final int minWordSize;
  private final int minSubwordSize;
  private final int maxSubwordSize;
  private final boolean onlyLongestMatch;

  /** Creates a new DictionaryCompoundWordTokenFilterFactory */
  public DictionaryCompoundWordTokenFilterFactory(Map<String, String> args) {
    super(args);
    assureMatchVersion();
    dictFile = require(args, "dictionary");
    minWordSize = getInt(args, "minWordSize", CompoundWordTokenFilterBase.DEFAULT_MIN_WORD_SIZE);
    minSubwordSize = getInt(args, "minSubwordSize", CompoundWordTokenFilterBase.DEFAULT_MIN_SUBWORD_SIZE);
    maxSubwordSize = getInt(args, "maxSubwordSize", CompoundWordTokenFilterBase.DEFAULT_MAX_SUBWORD_SIZE);
    onlyLongestMatch = getBoolean(args, "onlyLongestMatch", true);
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }
  
  @Override
  public void inform(ResourceLoader loader) throws IOException {
    dictionary = super.getWordSet(loader, dictFile, false);
  }
  
  @Override
  public TokenStream create(TokenStream input) {
    // if the dictionary is null, it means it was empty
    return dictionary == null ? input : new DictionaryCompoundWordTokenFilter
        (luceneMatchVersion, input, dictionary, minWordSize, minSubwordSize, maxSubwordSize, onlyLongestMatch);
  }
}

