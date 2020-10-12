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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.TokenFilterFactory;

/**
 * Factory for {@link TypeAsSynonymFilter}.
 *
 * <p>In Solr this might be used as such
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
 * the emitted synonym will have text "_type_&lt;URL&gt;". Note that this class does not attempt
 * to create a proper graph of synonyms so sausagization my occur. Contributions welcome on that
 * front.
 *
 * @since 8.6.0
 * @lucene.spi {@value #NAME}
 */
public class TypeAsSynonymFilterFactory extends TokenFilterFactory {

  /** SPI name */
  public static final String NAME = "typeAsSynonym";

  private final String prefix;
  private Set<String> ignore = null;
  private int synFlagMask;

  public TypeAsSynonymFilterFactory(Map<String,String> args) {
    super(args);
    prefix = get(args, "prefix");  // default value is null
    String ignoreList = get(args, "ignore");
    synFlagMask = getInt(args,"synFlagsMask", ~0);
    if (ignoreList != null) {
      ignore = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(ignoreList.split(","))));
    }
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  /** Default ctor for compatibility with SPI */
  public TypeAsSynonymFilterFactory() {
    throw defaultCtorException();
  }

  @Override
  public TokenStream create(TokenStream input) {
    return new TypeAsSynonymFilter(input, prefix, ignore, synFlagMask);
  }
}
