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

package org.apache.lucene.luke.models.overview;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.WeakHashMap;
import java.util.stream.Collectors;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.misc.HighFreqTerms;

/**
 * An utility class that collects terms and their statistics in a specific field.
 */
final class TopTerms {

  private final IndexReader reader;

  private final Map<String, List<TermStats>> topTermsCache;

  TopTerms(IndexReader reader) {
    this.reader = Objects.requireNonNull(reader);
    this.topTermsCache = new WeakHashMap<>();
  }

  /**
   * Returns the top indexed terms with their statistics for the specified field.
   *
   * @param field - the field name
   * @param numTerms - the max number of terms to be returned
   * @throws Exception - if an error occurs when collecting term statistics
   */
  List<TermStats> getTopTerms(String field, int numTerms) throws Exception {

    if (!topTermsCache.containsKey(field) || topTermsCache.get(field).size() < numTerms) {
      org.apache.lucene.misc.TermStats[] stats =
          HighFreqTerms.getHighFreqTerms(reader, numTerms, field, new HighFreqTerms.DocFreqComparator());

      List<TermStats> topTerms = Arrays.stream(stats)
          .map(TermStats::of)
          .collect(Collectors.toList());

      // cache computed statistics for later uses
      topTermsCache.put(field, topTerms);
    }

    return Collections.unmodifiableList(topTermsCache.get(field));
  }
}
