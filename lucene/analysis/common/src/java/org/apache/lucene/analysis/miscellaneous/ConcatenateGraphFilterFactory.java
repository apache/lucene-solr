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
package org.apache.lucene.analysis.miscellaneous;

import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;

/**
 * Factory for {@link ConcatenateGraphFilter}.
 *
 * <ul>
 *   <li><tt>preserveSep</tt>:
 *                            Whether {@link ConcatenateGraphFilter#SEP_LABEL}
 *                            should separate the input tokens in the concatenated token
 *                            </li>
 *   <li><tt>preservePositionIncrements</tt>:
 *                                       Whether to add an empty token for missing positions.
 *                                       The effect is a consecutive {@link ConcatenateGraphFilter#SEP_LABEL}.
 *                                       When false, it's as if there were no missing positions
 *                                         (we pretend the surrounding tokens were adjacent).
 *                                       </li>
 *   <li><tt>maxGraphExpansions</tt>:
 *                            If the tokenStream graph has more than this many possible paths through, then we'll throw
 *                            {@link TooComplexToDeterminizeException} to preserve the stability and memory of the
 *                            machine.
 *                            </li>
 * </ul>
 * @see ConcatenateGraphFilter
 * @since 7.4.0
 */
public class ConcatenateGraphFilterFactory extends TokenFilterFactory {

  private boolean preserveSep;
  private boolean preservePositionIncrements;
  private int maxGraphExpansions;

  public ConcatenateGraphFilterFactory(Map<String, String> args) {
    super(args);

    preserveSep = getBoolean(args, "preserveSep", ConcatenateGraphFilter.DEFAULT_PRESERVE_SEP);
    preservePositionIncrements = getBoolean(args, "preservePositionIncrements", ConcatenateGraphFilter.DEFAULT_PRESERVE_POSITION_INCREMENTS);
    maxGraphExpansions = getInt(args, "maxGraphExpansions", ConcatenateGraphFilter.DEFAULT_MAX_GRAPH_EXPANSIONS);

    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  @Override
  public TokenStream create(TokenStream input) {
    return new ConcatenateGraphFilter(input, preserveSep, preservePositionIncrements, maxGraphExpansions);
  }
}
