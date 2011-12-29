package org.apache.solr.analysis;

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

import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.cjk.CJKBigramFilter;

/** 
 * Factory for {@link CJKBigramFilter}.
 * <pre class="prettyprint" >
 * &lt;fieldType name="text_cjk" class="solr.TextField"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.StandardTokenizerFactory"/&gt;
 *     &lt;filter class="solr.CJKWidthFilterFactory"/&gt;
 *     &lt;filter class="solr.LowerCaseFilterFactory"/&gt;
 *     &lt;filter class="solr.CJKBigramFilterFactory" 
 *       han="true" hiragana="true" 
 *       katakana="true" hangul="true" /&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 */
public class CJKBigramFilterFactory extends BaseTokenFilterFactory {
  int flags;

  @Override
  public void init(Map<String,String> args) {
    super.init(args);
    flags = 0;
    if (getBoolean("han", true)) {
      flags |= CJKBigramFilter.HAN;
    }
    if (getBoolean("hiragana", true)) {
      flags |= CJKBigramFilter.HIRAGANA;
    }
    if (getBoolean("katakana", true)) {
      flags |= CJKBigramFilter.KATAKANA;
    }
    if (getBoolean("hangul", true)) {
      flags |= CJKBigramFilter.HANGUL;
    }
  }
  
  @Override
  public TokenStream create(TokenStream input) {
    return new CJKBigramFilter(input, flags);
  }
}
