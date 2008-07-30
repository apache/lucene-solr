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
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.util.plugin.ResourceLoaderAware;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.io.File;
import java.io.File;
import java.io.IOException;

/**
 * @version $Id$
 * @since solr 1.3
 */
public class KeepWordFilterFactory extends BaseTokenFilterFactory implements ResourceLoaderAware {

  private Set<String> words;
  private boolean ignoreCase;

  @SuppressWarnings("unchecked")
  public void inform(ResourceLoader loader) {
    String wordFiles = args.get("words");
    ignoreCase = getBoolean("ignoreCase",false);

    if (wordFiles != null) {
      if (words == null)
        words = new HashSet<String>();
      try {
        java.io.File keepWordsFile = new File(wordFiles);
        if (keepWordsFile.exists()) {
          List<String> wlist = loader.getLines(wordFiles);
          words = StopFilter.makeStopSet(
              (String[])wlist.toArray(new String[0]), ignoreCase);
        } else  {
          List<String> files = StrUtils.splitFileNames(wordFiles);
          for (String file : files) {
            List<String> wlist = loader.getLines(file.trim());
            words.addAll(StopFilter.makeStopSet((String[])wlist.toArray(new String[0]), ignoreCase));
          }
        }
      } 
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Set the keep word list.
   * NOTE: if ignoreCase==true, the words are expected to be lowercase
   */
  public void setWords(Set<String> words) {
    this.words = words;
  }

  public void setIgnoreCase(boolean ignoreCase) {
    this.ignoreCase = ignoreCase;
  }
  
  public KeepWordFilter create(TokenStream input) {
    return new KeepWordFilter(input,words,ignoreCase);
  }

}
