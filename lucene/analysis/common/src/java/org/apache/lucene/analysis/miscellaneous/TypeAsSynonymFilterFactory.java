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
 * Factory for {@link TypeAsSynonymFilter}.
 * <pre class="prettyprint">
 * &lt;fieldType name="text_type_as_synonym" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.UAX29URLEmailTokenizerFactory"/&gt;
 *     &lt;filter class="solr.TypeAsSynonymFilterFactory" prefix="_type_" /&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 *
 * <p>
 * If the optional {@code prefix} parameter is used, the specified value will be prepended
 * to the type, e.g. with prefix="_type_", for a token "example.com" with type "&lt;URL&gt;",
 * the emitted synonym will have text "_type_&lt;URL&gt;".
 *
 * @since 7.3.0
 */
public class TypeAsSynonymFilterFactory extends TokenFilterFactory {
  private final String prefix;

  public TypeAsSynonymFilterFactory(Map<String,String> args) {
    super(args);
    prefix = get(args, "prefix");  // default value is null
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  @Override
  public TokenStream create(TokenStream input) {
    return new TypeAsSynonymFilter(input, prefix);
  }
}
