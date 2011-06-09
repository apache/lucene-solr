
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
import org.apache.lucene.analysis.fr.*;
import org.apache.lucene.analysis.util.CharArraySet;

import java.io.IOException;
import org.apache.lucene.analysis.TokenStream;

/**
 * Factory for {@link ElisionFilter}.
 * <pre class="prettyprint" >
 * &lt;fieldType name="text_elsn" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.StandardTokenizerFactory"/&gt;
 *     &lt;filter class="solr.LowerCaseFilterFactory"/&gt;
 *     &lt;filter class="solr.ElisionFilterFactory" articles="stopwordarticles.txt"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 *
 */
public class ElisionFilterFactory extends BaseTokenFilterFactory implements ResourceLoaderAware {

  private CharArraySet articles;

  public void inform(ResourceLoader loader) {
    String articlesFile = args.get("articles");

    if (articlesFile != null) {
      try {
        articles = getWordSet(loader, articlesFile, false);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public ElisionFilter create(TokenStream input) {
    assureMatchVersion();
    return articles == null ? new ElisionFilter(luceneMatchVersion,input) : 
        new ElisionFilter(luceneMatchVersion,input,articles);
  }
}

