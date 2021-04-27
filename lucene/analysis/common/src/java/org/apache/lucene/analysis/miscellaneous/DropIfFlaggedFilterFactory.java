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

/**
 * Provides a filter that will drop tokens matching a set of flags. This might be used if you had
 * both custom filters that identify tokens to be removed, but need to run before other filters that
 * want to see the token that will eventually be dropped. Alternately you might have separate flag setting
 * filters and then remove tokens that match a particular combination of those filters.<br>
 * <br>
 * In Solr this might be configured such as
 * <pre class="prettyprint">
 *     &lt;analyzer type="index"&gt;
 *       &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *       &lt;-- other filters --&gt;
 *       &lt;filter class="solr.DropIfFlaggedFilterFactory" dropFlags="9"/&gt;
 *     &lt;/analyzer&gt;
 * </pre>
 * The above would drop any token that had the first and fourth bit set.
 *
 * @since 8.6.0
 * @lucene.spi {@value #NAME}
 */
public class DropIfFlaggedFilterFactory  extends TokenFilterFactory {
  /**
   * SPI name
   */
  public static final String NAME = "dropIfFlagged";

  private final int dropFlags;

  /**
   * Initialize this factory via a set of key-value pairs.
   */
  public DropIfFlaggedFilterFactory(Map<String, String> args) {
    super(args);
    dropFlags = getInt(args,"dropFlags", 2);

  }

  @Override
  public TokenStream create(TokenStream input) {
    return new DropIfFlaggedFilter(input, dropFlags);
  }
}
