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
package org.apache.lucene.analysis.core;


import org.apache.lucene.analysis.util.AbstractAnalysisFactory;
import org.apache.lucene.analysis.util.CharTokenizer;
import org.apache.lucene.analysis.util.MultiTermAwareComponent;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.util.AttributeFactory;

import java.util.HashMap;
import java.util.Map;

import static org.apache.lucene.analysis.standard.StandardTokenizer.MAX_TOKEN_LENGTH_LIMIT;

/**
 * Factory for {@link LowerCaseTokenizer}.
 * <pre class="prettyprint">
 * &lt;fieldType name="text_lwrcase" class="solr.TextField" positionIncrementGap="100"&gt;
 * &lt;analyzer&gt;
 * &lt;tokenizer class="solr.LowerCaseTokenizerFactory" maxTokenLen="256"/&gt;
 * &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 * <p>
 * Options:
 * <ul>
 * <li>maxTokenLen: max token length, should be greater than 0 and less than MAX_TOKEN_LENGTH_LIMIT (1024*1024).
 *     It is rare to need to change this
 * else {@link CharTokenizer}::DEFAULT_MAX_WORD_LEN</li>
 * </ul>
 */
public class LowerCaseTokenizerFactory extends TokenizerFactory implements MultiTermAwareComponent {
  private final int maxTokenLen;

  /**
   * Creates a new LowerCaseTokenizerFactory
   */
  public LowerCaseTokenizerFactory(Map<String, String> args) {
    super(args);
    maxTokenLen = getInt(args, "maxTokenLen", CharTokenizer.DEFAULT_MAX_WORD_LEN);
    if (maxTokenLen > MAX_TOKEN_LENGTH_LIMIT || maxTokenLen <= 0) {
      throw new IllegalArgumentException("maxTokenLen must be greater than 0 and less than " + MAX_TOKEN_LENGTH_LIMIT + " passed: " + maxTokenLen);
    }
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  @Override
  public LowerCaseTokenizer create(AttributeFactory factory) {
    return new LowerCaseTokenizer(factory, maxTokenLen);
  }

  @Override
  public AbstractAnalysisFactory getMultiTermComponent() {
    Map<String,String> map = new HashMap<>(getOriginalArgs());
    map.remove("maxTokenLen"); //removing "maxTokenLen" argument for LowerCaseFilterFactory init
    return new LowerCaseFilterFactory(map);
  }
}
