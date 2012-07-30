package org.apache.lucene.analysis.util;

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

import java.io.IOException;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;

/**
 * Factory for {@link ElisionFilter}.
 * <pre class="prettyprint" >
 * &lt;fieldType name="text_elsn" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.StandardTokenizerFactory"/&gt;
 *     &lt;filter class="solr.LowerCaseFilterFactory"/&gt;
 *     &lt;filter class="solr.ElisionFilterFactory" 
 *       articles="stopwordarticles.txt" ignoreCase="true"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 *
 */
public class ElisionFilterFactory extends TokenFilterFactory implements ResourceLoaderAware {

  private CharArraySet articles;

  public void inform(ResourceLoader loader) throws IOException {
    String articlesFile = args.get("articles");
    boolean ignoreCase = getBoolean("ignoreCase", false);

    if (articlesFile != null) {
      articles = getWordSet(loader, articlesFile, ignoreCase);
    }
    if (articles == null) {
      articles = FrenchAnalyzer.DEFAULT_ARTICLES;
    }
  }

  public ElisionFilter create(TokenStream input) {
    return new ElisionFilter(input, articles);
  }
}

