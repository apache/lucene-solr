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

package org.apache.lucene.analysis.pattern;

import java.util.Map;

import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.util.AttributeFactory;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;

/**
 * Factory for {@link SimplePatternTokenizer}, for matching tokens based on the provided regexp.
 *
 * <p>This tokenizer uses Lucene {@link RegExp} pattern matching to construct distinct tokens
 * for the input stream.  The syntax is more limited than {@link PatternTokenizer}, but the
 * tokenization is quite a bit faster.  It takes two arguments:
 * <br>
 * <ul>
 * <li>"pattern" (required) is the regular expression, according to the syntax described at {@link RegExp}</li>
 * <li>"maxDeterminizedStates" (optional, default 10000) the limit on total state count for the determined automaton computed from the regexp</li>
 * </ul>
 * <p>
 * The pattern matches the characters to include in a token (not the split characters), and the
 * matching is greedy such that the longest token matching at a given point is created.  Empty
 * tokens are never created.
 *
 * <p>For example, to match tokens delimited by simple whitespace characters:
 *
 * <pre class="prettyprint">
 * &lt;fieldType name="text_ptn" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.SimplePatternTokenizerFactory" pattern="[^ \t\r\n]+"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre> 
 *
 * @lucene.experimental
 * 
 * @see SimplePatternTokenizer
 *
 * @since 6.5.0
 * @lucene.spi {@value #NAME}
 */
public class SimplePatternTokenizerFactory extends TokenizerFactory {

  /** SPI name */
  public static final String NAME = "simplePattern";

  public static final String PATTERN = "pattern";
  private final Automaton dfa;
  private final int maxDeterminizedStates;
 
  /** Creates a new SimplePatternTokenizerFactory */
  public SimplePatternTokenizerFactory(Map<String,String> args) {
    super(args);
    maxDeterminizedStates = getInt(args, "maxDeterminizedStates", Operations.DEFAULT_MAX_DETERMINIZED_STATES);
    dfa = Operations.determinize(new RegExp(require(args, PATTERN)).toAutomaton(), maxDeterminizedStates);
    if (args.isEmpty() == false) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }
  
  @Override
  public SimplePatternTokenizer create(final AttributeFactory factory) {
    return new SimplePatternTokenizer(factory, dfa);
  }
}
