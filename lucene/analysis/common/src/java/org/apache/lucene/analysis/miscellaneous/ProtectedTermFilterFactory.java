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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate; // javadocs

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.analysis.util.TokenFilterFactory;

/**
 * Factory for a {@link ProtectedTermFilter}
 *
 * <p>CustomAnalyzer example:
 * <pre class="prettyprint">
 * Analyzer ana = CustomAnalyzer.builder()
 *   .withTokenizer("standard")
 *   .when("protectedterm", "ignoreCase", "true", "protected", "protectedTerms.txt")
 *     .addTokenFilter("truncate", "prefixLength", "4")
 *     .addTokenFilter("lowercase")
 *   .endwhen()
 *   .build();
 * </pre>
 *
 * <p>Solr example, in which conditional filters are specified via the <code>wrappedFilters</code>
 * parameter - a comma-separated list of case-insensitive TokenFilter SPI names - and conditional
 * filter args are specified via <code>filterName.argName</code> parameters:
 * <pre class="prettyprint">
 * &lt;fieldType name="reverse_lower_with_exceptions" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *     &lt;filter class="solr.ProtectedTermFilterFactory" ignoreCase="true" protected="protectedTerms.txt"
 *             wrappedFilters="truncate,lowercase" truncate.prefixLength="4" /&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 *
 * <p>When using the <code>wrappedFilters</code> parameter, each filter name must be unique, so if you
 * need to specify the same filter more than once, you must add case-insensitive unique '-id' suffixes
 * (note that the '-id' suffix is stripped prior to SPI lookup), e.g.:
 * <pre class="prettyprint">
 * &lt;fieldType name="double_synonym_with_exceptions" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *     &lt;filter class="solr.ProtectedTermFilterFactory" ignoreCase="true" protected="protectedTerms.txt"
 *             wrappedFilters="synonymgraph-A,synonymgraph-B"
 *             synonymgraph-A.synonyms="synonyms-1.txt"
 *             synonymgraph-B.synonyms="synonyms-2.txt"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 *
 * <p>See related {@link org.apache.lucene.analysis.custom.CustomAnalyzer.Builder#whenTerm(Predicate)}
 *
 * @since 7.4.0
 * @lucene.spi {@value #NAME}
 */
public class ProtectedTermFilterFactory extends ConditionalTokenFilterFactory implements ResourceLoaderAware {

  public static final String NAME = "protectedTerm";

  public static final String PROTECTED_TERMS = "protected";
  public static final char FILTER_ARG_SEPARATOR = '.';
  public static final char FILTER_NAME_ID_SEPARATOR = '-';

  private final String termFiles;
  private final boolean ignoreCase;
  private final String wrappedFilters;

  private CharArraySet protectedTerms;

  public ProtectedTermFilterFactory(Map<String, String> args) {
    super(args);
    termFiles = require(args, PROTECTED_TERMS);
    ignoreCase = getBoolean(args, "ignoreCase", false);
    wrappedFilters = get(args, "wrappedFilters");
    if (wrappedFilters != null) {
      handleWrappedFilterArgs(args);
    }
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  private void handleWrappedFilterArgs(Map<String, String> args) {
    LinkedHashMap<String, Map<String, String>> wrappedFilterArgs = new LinkedHashMap<>();
    splitAt(',', wrappedFilters).forEach(filterName -> {          // Format: SPIname[-id]
      filterName = filterName.trim().toLowerCase(Locale.ROOT);             // Treat case-insensitively
      if (wrappedFilterArgs.containsKey(filterName)) {
        throw new IllegalArgumentException("wrappedFilters contains duplicate '"
            + filterName + "'. Add unique '-id' suffixes (stripped prior to SPI lookup).");
      }
      wrappedFilterArgs.put(filterName, new HashMap<>());
    });
    for (Iterator<Map.Entry<String, String>> iterator = args.entrySet().iterator(); iterator.hasNext(); ) {
      Map.Entry<String, String> entry = iterator.next();
      String filterArgKey = entry.getKey();
      String argValue = entry.getValue();
      List<String> splitKey = splitAt(FILTER_ARG_SEPARATOR, filterArgKey); // Format: filterName.argKey
      if (splitKey.size() == 2) {                                          // Skip if no slash
        String filterName = splitKey.get(0).toLowerCase(Locale.ROOT);
        if (wrappedFilterArgs.containsKey(filterName)) {                   // Skip if not in "wrappedFilter" arg
          Map<String, String> filterArgs = wrappedFilterArgs.computeIfAbsent(filterName, k -> new HashMap<>());
          String argKey = splitKey.get(1);
          filterArgs.put(argKey, argValue); // argKey is guaranteed unique, don't need to check for duplicates
          iterator.remove();
        }
      }
    }
    if (args.isEmpty()) {
      populateInnerFilters(wrappedFilterArgs);
    }
  }

  private void populateInnerFilters(LinkedHashMap<String, Map<String, String>> wrappedFilterArgs) {
    List<TokenFilterFactory> innerFilters = new ArrayList<>();
    wrappedFilterArgs.forEach((filterName, filterArgs) -> {
      int idSuffixPos = filterName.indexOf(FILTER_NAME_ID_SEPARATOR); // Format: SPIname[-id]
      if (idSuffixPos != -1) {                                        // Strip '-id' suffix, if any, prior to SPI lookup
        filterName = filterName.substring(0, idSuffixPos);
      }
      innerFilters.add(TokenFilterFactory.forName(filterName, filterArgs));
    });
    setInnerFilters(innerFilters);
  }

  public boolean isIgnoreCase() {
    return ignoreCase;
  }

  public CharArraySet getProtectedTerms() {
    return protectedTerms;
  }

  @Override
  protected ConditionalTokenFilter create(TokenStream input, Function<TokenStream, TokenStream> inner) {
    return new ProtectedTermFilter(protectedTerms, input, inner);
  }

  @Override
  public void doInform(ResourceLoader loader) throws IOException {
    protectedTerms = getWordSet(loader, termFiles, ignoreCase);
  }
}
