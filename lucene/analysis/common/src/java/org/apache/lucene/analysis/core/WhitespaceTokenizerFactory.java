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

import static org.apache.lucene.analysis.standard.StandardTokenizer.MAX_TOKEN_LENGTH_LIMIT;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.TokenizerFactory;
import org.apache.lucene.analysis.util.CharTokenizer;
import org.apache.lucene.util.AttributeFactory;

/**
 * Factory for {@link WhitespaceTokenizer}.
 *
 * <pre class="prettyprint">
 * &lt;fieldType name="text_ws" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory" rule="unicode"  maxTokenLen="256"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 *
 * Options:
 *
 * <ul>
 *   <li>rule: either "java" for {@link WhitespaceTokenizer} or "unicode" for {@link
 *       UnicodeWhitespaceTokenizer}
 *   <li>maxTokenLen: max token length, should be greater than 0 and less than
 *       MAX_TOKEN_LENGTH_LIMIT (1024*1024). It is rare to need to change this else {@link
 *       CharTokenizer}::DEFAULT_MAX_TOKEN_LEN
 * </ul>
 *
 * @since 3.1
 * @lucene.spi {@value #NAME}
 */
public class WhitespaceTokenizerFactory extends TokenizerFactory {

  /** SPI name */
  public static final String NAME = "whitespace";

  public static final String RULE_JAVA = "java";
  public static final String RULE_UNICODE = "unicode";
  private static final Collection<String> RULE_NAMES = Arrays.asList(RULE_JAVA, RULE_UNICODE);

  private final String rule;
  private final int maxTokenLen;

  /** Creates a new WhitespaceTokenizerFactory */
  public WhitespaceTokenizerFactory(Map<String, String> args) {
    super(args);

    rule = get(args, "rule", RULE_NAMES, RULE_JAVA);
    maxTokenLen = getInt(args, "maxTokenLen", CharTokenizer.DEFAULT_MAX_WORD_LEN);
    if (maxTokenLen > MAX_TOKEN_LENGTH_LIMIT || maxTokenLen <= 0) {
      throw new IllegalArgumentException(
          "maxTokenLen must be greater than 0 and less than "
              + MAX_TOKEN_LENGTH_LIMIT
              + " passed: "
              + maxTokenLen);
    }
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  /** Default ctor for compatibility with SPI */
  public WhitespaceTokenizerFactory() {
    throw defaultCtorException();
  }

  @Override
  public Tokenizer create(AttributeFactory factory) {
    switch (rule) {
      case RULE_JAVA:
        return new WhitespaceTokenizer(factory, maxTokenLen);
      case RULE_UNICODE:
        return new UnicodeWhitespaceTokenizer(factory, maxTokenLen);
      default:
        throw new AssertionError();
    }
  }
}
