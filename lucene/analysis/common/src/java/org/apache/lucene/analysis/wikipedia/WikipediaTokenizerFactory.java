package org.apache.lucene.analysis.wikipedia;

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

import java.util.Collections;
import java.util.Map;

import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.util.AttributeFactory;

/** 
 * Factory for {@link WikipediaTokenizer}.
 * <pre class="prettyprint">
 * &lt;fieldType name="text_wiki" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WikipediaTokenizerFactory"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 */
public class WikipediaTokenizerFactory extends TokenizerFactory {
  
  /** Creates a new WikipediaTokenizerFactory */
  public WikipediaTokenizerFactory(Map<String,String> args) {
    super(args);
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }
  
  // TODO: add support for WikipediaTokenizer's advanced options.
  @Override
  public WikipediaTokenizer create(AttributeFactory factory) {
    return new WikipediaTokenizer(factory, WikipediaTokenizer.TOKENS_ONLY,
        Collections.<String>emptySet());
  }
}
