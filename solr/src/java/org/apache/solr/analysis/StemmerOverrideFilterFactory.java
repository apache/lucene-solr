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
import java.util.List;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.StemmerOverrideFilter;
import org.apache.lucene.analysis.util.CharArrayMap;
import org.apache.solr.common.ResourceLoader;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.util.plugin.ResourceLoaderAware;

/**
 * Factory for {@link StemmerOverrideFilter}.
 * <pre class="prettyprint" >
 * &lt;fieldType name="text_dicstem" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *     &lt;filter class="solr.StemmerOverrideFilterFactory" dictionary="dictionary.txt" ignoreCase="false"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 *
 */
public class StemmerOverrideFilterFactory extends BaseTokenFilterFactory implements ResourceLoaderAware {
  private CharArrayMap<String> dictionary = null;
  private boolean ignoreCase;

  public void inform(ResourceLoader loader) {
    String dictionaryFiles = args.get("dictionary");
    ignoreCase = getBoolean("ignoreCase", false);
    if (dictionaryFiles != null) {
      assureMatchVersion();
      List<String> files = StrUtils.splitFileNames(dictionaryFiles);
      try {
        if (files.size() > 0) {
          dictionary = new CharArrayMap<String>(luceneMatchVersion, 
              files.size() * 10, ignoreCase);
          for (String file : files) {
            List<String> list = loader.getLines(file.trim());
            for (String line : list) {
              String[] mapping = line.split("\t", 2);
              dictionary.put(mapping[0], mapping[1]);
            }
          }
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public boolean isIgnoreCase() {
    return ignoreCase;
  }

  public TokenStream create(TokenStream input) {
    return dictionary == null ? input : new StemmerOverrideFilter(luceneMatchVersion, input, dictionary);
  }
}
