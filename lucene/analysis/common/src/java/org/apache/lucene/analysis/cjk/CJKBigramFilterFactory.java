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
package org.apache.lucene.analysis.cjk;


import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.cjk.CJKBigramFilter;
import org.apache.lucene.analysis.util.TokenFilterFactory;

/** 
 * Factory for {@link CJKBigramFilter}.
 * <pre class="prettyprint">
 * &lt;fieldType name="text_cjk" class="solr.TextField"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.StandardTokenizerFactory"/&gt;
 *     &lt;filter class="solr.CJKWidthFilterFactory"/&gt;
 *     &lt;filter class="solr.LowerCaseFilterFactory"/&gt;
 *     &lt;filter class="solr.CJKBigramFilterFactory" 
 *       han="true" hiragana="true" 
 *       katakana="true" hangul="true" outputUnigrams="false" /&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 */
public class CJKBigramFilterFactory extends TokenFilterFactory {
  final int flags;
  final boolean outputUnigrams;

  /** Creates a new CJKBigramFilterFactory */
  public CJKBigramFilterFactory(Map<String,String> args) {
    super(args);
    int flags = 0;
    if (getBoolean(args, "han", true)) {
      flags |= CJKBigramFilter.HAN;
    }
    if (getBoolean(args, "hiragana", true)) {
      flags |= CJKBigramFilter.HIRAGANA;
    }
    if (getBoolean(args, "katakana", true)) {
      flags |= CJKBigramFilter.KATAKANA;
    }
    if (getBoolean(args, "hangul", true)) {
      flags |= CJKBigramFilter.HANGUL;
    }
    this.flags = flags;
    this.outputUnigrams = getBoolean(args, "outputUnigrams", false);
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }
  
  @Override
  public TokenStream create(TokenStream input) {
    return new CJKBigramFilter(input, flags, outputUnigrams);
  }
}
