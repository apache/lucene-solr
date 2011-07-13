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

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.KeywordMarkerFilter;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.apache.solr.common.ResourceLoader;
import org.apache.solr.util.plugin.ResourceLoaderAware;

import java.io.IOException;
import java.util.Map;

/**
 * @version $Id$
 *
 * @deprecated Use SnowballPorterFilterFactory with language="English" instead
 */
@Deprecated
public class EnglishPorterFilterFactory extends BaseTokenFilterFactory implements ResourceLoaderAware {
  public static final String PROTECTED_TOKENS = "protected";

  @Override
  public void init(Map<String,String> args) {
    super.init(args);
    warnDeprecated("use PorterStemFilterFactory (Porter1) or SnowballPorterFilterFactory with 'English' (Porter2) instead");
  }
  
  public void inform(ResourceLoader loader) {
    String wordFiles = args.get(PROTECTED_TOKENS);
    if (wordFiles != null) {
      try {
        protectedWords = getWordSet(loader, wordFiles, false);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private CharArraySet protectedWords = null;

  public TokenFilter create(TokenStream input) {
    if (protectedWords != null)
      input = new KeywordMarkerFilter(input, protectedWords);
    return new SnowballFilter(input, new org.tartarus.snowball.ext.EnglishStemmer());
  }

}
