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

import org.apache.solr.common.ResourceLoader;
import org.apache.solr.util.plugin.ResourceLoaderAware;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.CharArraySet;

import java.util.Set;
import java.io.IOException;

/**
 * @version $Id$
 * @since solr 1.3
 */
public class KeepWordFilterFactory extends BaseTokenFilterFactory implements ResourceLoaderAware {

  private CharArraySet words;
  private boolean ignoreCase;

  public void inform(ResourceLoader loader) {
    String wordFiles = args.get("words");
    ignoreCase = getBoolean("ignoreCase", false);
    if (wordFiles != null) {   
      try {
        words = getWordSet(loader, wordFiles, ignoreCase);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Set the keep word list.
   * NOTE: if ignoreCase==true, the words are expected to be lowercase
   */
  public void setWords(Set<String> words) {
    this.words = new CharArraySet(luceneMatchVersion, words, ignoreCase);
  }

  public void setIgnoreCase(boolean ignoreCase) {    
    if (words != null && this.ignoreCase != ignoreCase) {
      words = new CharArraySet(luceneMatchVersion, words, ignoreCase);
    }
    this.ignoreCase = ignoreCase;
  }

  public KeepWordFilter create(TokenStream input) {
    return new KeepWordFilter(input, words);
  }

  public CharArraySet getWords() {
    return words;
  }

  public boolean isIgnoreCase() {
    return ignoreCase;
  }
}
