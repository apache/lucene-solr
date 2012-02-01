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

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.kuromoji.KuromojiPartOfSpeechStopFilter;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.solr.common.ResourceLoader;
import org.apache.solr.util.plugin.ResourceLoaderAware;

/**
 * Factory for {@link KuromojiPartOfSpeechStopFilter}.  
 * <pre class="prettyprint">
 * &lt;fieldType name="text_ja" class="solr.TextField"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.KuromojiTokenizerFactory"/&gt;
 *     &lt;filter class="solr.KuromojiPartOfSpeechStopFilterFactory" 
 *             tags="stopTags.txt" 
 *             enablePositionIncrements="true"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;
 * </pre>
 */
public class KuromojiPartOfSpeechStopFilterFactory extends BaseTokenFilterFactory implements ResourceLoaderAware  {
  private boolean enablePositionIncrements;
  private Set<String> stopTags;

  public void inform(ResourceLoader loader) {
    String stopTagFiles = args.get("tags");
    enablePositionIncrements = getBoolean("enablePositionIncrements", false);
    try {
      CharArraySet cas = getWordSet(loader, stopTagFiles, false);
      stopTags = new HashSet<String>();
      for (Object element : cas) {
        char chars[] = (char[]) element;
        stopTags.add(new String(chars));
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public TokenStream create(TokenStream stream) {
    return new KuromojiPartOfSpeechStopFilter(enablePositionIncrements, stream, stopTags);
  }
}
