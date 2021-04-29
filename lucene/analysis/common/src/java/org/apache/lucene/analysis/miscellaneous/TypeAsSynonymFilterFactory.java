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
import java.util.Set;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.TokenFilterFactory;

/**
 * Factory for {@link TypeAsSynonymFilter}.
 *
 * <p>In Solr this might be used as such
 * <pre class="prettyprint">
 * &lt;fieldType name="text_type_as_synonym" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.UAX29URLEmailTokenizerFactory"/&gt;
 *     &lt;filter class="solr.TypeAsSynonymFilterFactory" prefix="_type_" synFlagsMask="5" ignore="foo,bar"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 *
 * <p>
 * If the optional {@code prefix} parameter is used, the specified value will be prepended
 * to the type, e.g. with prefix="_type_", for a token "example.com" with type "&lt;URL&gt;",
 * the emitted synonym will have text "_type_&lt;URL&gt;". If the optional synFlagsMask is used
 * then the flags on the synonym will be set to <code>synFlagsMask &amp; tokenFlags</code>. The
 * example above transfers only the lowest and third lowest bits. If no mask is set then
 * all flags are transferred. The ignore parameter can be used to avoid creating synonyms for
 * some types.
 *
 * @since 7.3.0
 * @lucene.spi {@value #NAME}
 */
public class TypeAsSynonymFilterFactory extends TokenFilterFactory {

  /** SPI name */
  public static final String NAME = "typeAsSynonym";

  private final String prefix;
  private final Set<String> ignore;
  private final int synFlagMask;

  public TypeAsSynonymFilterFactory(Map<String,String> args) {
    super(args);
    prefix = get(args, "prefix");  // default value is null
    ignore = getSet(args, "ignore");
    synFlagMask = getInt(args,"synFlagsMask", ~0);
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  @Override
  public TokenStream create(TokenStream input) {
    return new TypeAsSynonymFilter(input, prefix, ignore, synFlagMask);
  }
}
