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
package org.apache.lucene.analysis.miscellaneous;


import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.StemmerOverrideFilter.StemmerOverrideMap;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.analysis.util.TokenFilterFactory;

/**
 * Factory for {@link StemmerOverrideFilter}.
 * <pre class="prettyprint">
 * &lt;fieldType name="text_dicstem" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *     &lt;filter class="solr.StemmerOverrideFilterFactory" dictionary="dictionary.txt" ignoreCase="false"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 * @since 3.1.0
 * @lucene.spi {@value #NAME}
 */
public class StemmerOverrideFilterFactory extends TokenFilterFactory implements ResourceLoaderAware {

  /** SPI name */
  public static final String NAME = "stemmerOverride";

  private StemmerOverrideMap dictionary;
  private final String dictionaryFiles;
  private final boolean ignoreCase;

  /** Creates a new StemmerOverrideFilterFactory */
  public StemmerOverrideFilterFactory(Map<String,String> args) {
    super(args);
    dictionaryFiles = get(args, "dictionary");
    ignoreCase = getBoolean(args, "ignoreCase", false);
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  @Override
  public void inform(ResourceLoader loader) throws IOException {
    if (dictionaryFiles != null) {
      List<String> files = splitFileNames(dictionaryFiles);
      if (files.size() > 0) {
        StemmerOverrideFilter.Builder builder = new StemmerOverrideFilter.Builder(ignoreCase);
        for (String file : files) {
          List<String> list = getLines(loader, file.trim());
          for (String line : list) {
            String[] mapping = line.split("\t", 2);
            builder.add(mapping[0], mapping[1]);
          }
        }
        dictionary = builder.build();
      }
    }
  }

  public boolean isIgnoreCase() {
    return ignoreCase;
  }

  @Override
  public TokenStream create(TokenStream input) {
    return dictionary == null ? input : new StemmerOverrideFilter(input, dictionary);
  }
}
