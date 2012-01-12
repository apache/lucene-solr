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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.kuromoji.KuromojiBaseFormFilter;

/**
 * Factory for {@link KuromojiBaseFormFilter}.  
 * <pre class="prettyprint">
 * &lt;fieldType name="text_ja" class="solr.TextField"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.KuromojiTokenizerFactory"/&gt;
 *     &lt;filter class="solr.KuromojiBaseFormFilterFactory"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;
 * </pre>
 */
public class KuromojiBaseFormFilterFactory extends BaseTokenFilterFactory {

  @Override
  public TokenStream create(TokenStream input) {
    return new KuromojiBaseFormFilter(input);
  }
}
