/*
 This software was produced for the U. S. Government
 under Contract No. W15P7T-11-C-F600, and is
 subject to the Rights in Noncommercial Computer Software
 and Noncommercial Computer Software Documentation
 Clause 252.227-7014 (JUN 1995)

 Copyright 2013 The MITRE Corporation. All Rights Reserved.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package org.apache.lucene.analysis.miscellaneous;

import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.TokenFilterFactory;

/**
 * Factory for {@link ConcatenateGraphFilter}.
 *
 * <pre class="prettyprint">
 * nocommit update
 * </pre>
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
