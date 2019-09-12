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
package org.apache.lucene.analysis.wikipedia;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

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
 *
 * @since 3.1
 * @lucene.spi {@value #NAME}
 */
public class WikipediaTokenizerFactory extends TokenizerFactory {

  /** SPI name */
  public static final String NAME = "wikipedia";

  public static final String TOKEN_OUTPUT = "tokenOutput";
  public static final String UNTOKENIZED_TYPES = "untokenizedTypes";

  protected final int tokenOutput;
  protected Set<String> untokenizedTypes;

  /** Creates a new WikipediaTokenizerFactory */
  public WikipediaTokenizerFactory(Map<String,String> args) {
    super(args);
    tokenOutput = getInt(args, TOKEN_OUTPUT, WikipediaTokenizer.TOKENS_ONLY);
    untokenizedTypes = getSet(args, UNTOKENIZED_TYPES);

    if (untokenizedTypes == null) {
      untokenizedTypes = Collections.emptySet();
    }
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  @Override
  public WikipediaTokenizer create(AttributeFactory factory) {
    return new WikipediaTokenizer(factory, tokenOutput, untokenizedTypes);
  }
}
