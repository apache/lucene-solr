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
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.analysis.TokenStream;

import java.util.List;
import java.util.Set;
import java.io.IOException;

/**
 * @version $Id$
 */
public class StopFilterFactory extends BaseTokenFilterFactory implements ResourceLoaderAware {

  public void inform(ResourceLoader loader) {
    String stopWordFile = args.get("words");
    ignoreCase = getBoolean("ignoreCase",false);

    if (stopWordFile != null) {
      try {
        List<String> wlist = loader.getLines(stopWordFile);
        stopWords = StopFilter.makeStopSet((String[])wlist.toArray(new String[0]), ignoreCase);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private Set stopWords = StopFilter.makeStopSet(StopAnalyzer.ENGLISH_STOP_WORDS);
  private boolean ignoreCase;

  public StopFilter create(TokenStream input) {
    return new StopFilter(input,stopWords,ignoreCase);
  }
}
