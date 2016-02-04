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
package org.apache.lucene.analysis.cn.smart;

import java.util.Map;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.cn.smart.WordTokenFilter;
import org.apache.lucene.analysis.util.TokenFilterFactory;

/**
 * Factory for the SmartChineseAnalyzer {@link WordTokenFilter}
 * <p>
 * Note: this class will currently emit tokens for punctuation. So you should either add
 * a WordDelimiterFilter after to remove these (with concatenate off), or use the 
 * SmartChinese stoplist with a StopFilterFactory via:
 * <code>words="org/apache/lucene/analysis/cn/smart/stopwords.txt"</code>
 * @lucene.experimental
 * @deprecated Use {@link HMMChineseTokenizerFactory} instead
 */
@Deprecated
public class SmartChineseWordTokenFilterFactory extends TokenFilterFactory {
  
  /** Creates a new SmartChineseWordTokenFilterFactory */
  public SmartChineseWordTokenFilterFactory(Map<String,String> args) {
    super(args);
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }
  
  @Override
  public TokenFilter create(TokenStream input) {
      return new WordTokenFilter(input);
  }
}
