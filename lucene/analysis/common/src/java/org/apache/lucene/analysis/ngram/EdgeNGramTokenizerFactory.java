package org.apache.lucene.analysis.ngram;

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

import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.util.AttributeSource.AttributeFactory;

import java.io.Reader;
import java.util.Map;

/**
 * Creates new instances of {@link EdgeNGramTokenizer}.
 * <pre class="prettyprint">
 * &lt;fieldType name="text_edgngrm" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.EdgeNGramTokenizerFactory" side="front" minGramSize="1" maxGramSize="1"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 */
public class EdgeNGramTokenizerFactory extends TokenizerFactory {
  private final int maxGramSize;
  private final int minGramSize;
  private final String side;

  /** Creates a new EdgeNGramTokenizerFactory */
  public EdgeNGramTokenizerFactory(Map<String, String> args) {
    super(args);
    minGramSize = getInt(args, "minGramSize", EdgeNGramTokenizer.DEFAULT_MIN_GRAM_SIZE);
    maxGramSize = getInt(args, "maxGramSize", EdgeNGramTokenizer.DEFAULT_MAX_GRAM_SIZE);
    side = get(args, "side", EdgeNGramTokenFilter.Side.FRONT.getLabel());
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }
  
  @Override
  public EdgeNGramTokenizer create(AttributeFactory factory, Reader input) {
    return new EdgeNGramTokenizer(factory, input, side, minGramSize, maxGramSize);
  }
}
