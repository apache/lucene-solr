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

package org.apache.lucene.spatial.util;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredDocIdSet;
import org.apache.lucene.util.Bits;

import java.io.IOException;

/**
 * Filter that matches all documents where a ValueSource is
 * in between a range of <code>min</code> and <code>max</code> inclusive.
 * @lucene.internal
 */
public class ValueSourceFilter extends Filter {
  //TODO see https://issues.apache.org/jira/browse/LUCENE-4251  (move out of spatial & improve)

  final Filter startingFilter;
  final ValueSource source;
  final double min;
  final double max;

  public ValueSourceFilter( Filter startingFilter, ValueSource source, double min, double max )
  {
    if (startingFilter == null) {
      throw new IllegalArgumentException("please provide a non-null startingFilter; you can use QueryWrapperFilter(MatchAllDocsQuery) as a no-op filter");
    }
    this.startingFilter = startingFilter;
    this.source = source;
    this.min = min;
    this.max = max;
  }

  @Override
  public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
    final FunctionValues values = source.getValues( null, context );
    return new FilteredDocIdSet(startingFilter.getDocIdSet(context, acceptDocs)) {
      @Override
      public boolean match(int doc) {
        double val = values.doubleVal( doc );
        return val >= min && val <= max;
      }
    };
  }
}
