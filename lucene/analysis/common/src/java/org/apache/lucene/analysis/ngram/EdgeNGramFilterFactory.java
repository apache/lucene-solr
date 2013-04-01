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

import java.util.Map;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.TokenFilterFactory;

/**
 * Creates new instances of {@link EdgeNGramTokenFilter}.
 * <pre class="prettyprint">
 * &lt;fieldType name="text_edgngrm" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *     &lt;filter class="solr.EdgeNGramFilterFactory" side="front" minGramSize="1" maxGramSize="1"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 */
public class EdgeNGramFilterFactory extends TokenFilterFactory {
  private final int maxGramSize;
  private final int minGramSize;
  private final String side;

  /** Creates a new EdgeNGramFilterFactory */
  public EdgeNGramFilterFactory(Map<String, String> args) {
    super(args);
    minGramSize = getInt(args, "minGramSize", EdgeNGramTokenFilter.DEFAULT_MIN_GRAM_SIZE);
    maxGramSize = getInt(args, "maxGramSize", EdgeNGramTokenFilter.DEFAULT_MAX_GRAM_SIZE);
    side = get(args, "side", EdgeNGramTokenFilter.Side.FRONT.getLabel());
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  @Override
  public EdgeNGramTokenFilter create(TokenStream input) {
    return new EdgeNGramTokenFilter(input, side, minGramSize, maxGramSize);
  }
}
