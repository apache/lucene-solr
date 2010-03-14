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
import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.CharArraySet;

import java.util.HashSet;
import java.util.List;
import java.io.File;
import java.util.Set;
import java.io.File;
import java.io.IOException;

/**
 * @version $Id$
 */
public class StopFilterFactory extends BaseTokenFilterFactory implements ResourceLoaderAware {

  public void inform(ResourceLoader loader) {
    String stopWordFiles = args.get("words");
    ignoreCase = getBoolean("ignoreCase",false);
    enablePositionIncrements = getBoolean("enablePositionIncrements",false);

    if (stopWordFiles != null) {
      try {
        List<String> files = StrUtils.splitFileNames(stopWordFiles);
          if (stopWords == null && files.size() > 0){
            //default stopwords list has 35 or so words, but maybe don't make it that big to start
            stopWords = new CharArraySet(files.size() * 10, ignoreCase);
          }
          for (String file : files) {
            List<String> wlist = loader.getLines(file.trim());
            //TODO: once StopFilter.makeStopSet(List) method is available, switch to using that so we can avoid a toArray() call
            stopWords.addAll(StopFilter.makeStopSet((String[])wlist.toArray(new String[0]), ignoreCase));
          }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      stopWords = new CharArraySet(StopAnalyzer.ENGLISH_STOP_WORDS_SET, ignoreCase);
    }
  }
  //Force the use of a char array set, as it is the most performant, although this may break things if Lucene ever goes away from it.  See SOLR-1095
  private CharArraySet stopWords;
  private boolean ignoreCase;
  private boolean enablePositionIncrements;

  public boolean isEnablePositionIncrements() {
    return enablePositionIncrements;
  }

  public boolean isIgnoreCase() {
    return ignoreCase;
  }

  public Set getStopWords() {
    return stopWords;
  }

  public StopFilter create(TokenStream input) {
    StopFilter stopFilter = new StopFilter(enablePositionIncrements, input,stopWords,ignoreCase);
    return stopFilter;
  }
}
