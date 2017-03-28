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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;

/**
 * Caches the doubleVal of another value source in a HashMap
 * so that it is computed only once.
 * @lucene.internal
 */
public class CachingDoubleValueSource extends DoubleValuesSource {

  final DoubleValuesSource source;
  final Map<Integer, Double> cache;

  public CachingDoubleValueSource(DoubleValuesSource source) {
    this.source = source;
    cache = new HashMap<>();
  }

  @Override
  public String toString() {
    return "Cached["+source.toString()+"]";
  }

  @Override
  public DoubleValues getValues(LeafReaderContext readerContext, DoubleValues scores) throws IOException {
    final int base = readerContext.docBase;
    final DoubleValues vals = source.getValues(readerContext, scores);
    return new DoubleValues() {

      @Override
      public double doubleValue() throws IOException {
        int key = base + doc;
        Double v = cache.get(key);
        if (v == null) {
          v = vals.doubleValue();
          cache.put(key, v);
        }
        return v;
      }

      @Override
      public boolean advanceExact(int doc) throws IOException {
        this.doc = doc;
        return vals.advanceExact(doc);
      }

      int doc = -1;

    };
  }

  @Override
  public boolean needsScores() {
    return false;
  }

  @Override
  public boolean isCacheable(LeafReaderContext ctx) {
    return source.isCacheable(ctx);
  }

  @Override
  public Explanation explain(LeafReaderContext ctx, int docId, Explanation scoreExplanation) throws IOException {
    return source.explain(ctx, docId, scoreExplanation);
  }

  @Override
  public DoubleValuesSource rewrite(IndexSearcher searcher) throws IOException {
    return new CachingDoubleValueSource(source.rewrite(searcher));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    CachingDoubleValueSource that = (CachingDoubleValueSource) o;

    if (source != null ? !source.equals(that.source) : that.source != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return source != null ? source.hashCode() : 0;
  }
}
