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
package org.apache.lucene.queries.function.valuesource;

import java.io.IOException;
import java.util.Map;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.LongDocValues;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.BytesRef;

/**
 * <code>TotalTermFreqValueSource</code> returns the total term freq (sum of term freqs across all
 * documents).
 *
 * @lucene.internal
 */
public class TotalTermFreqValueSource extends ValueSource {
  protected final String field;
  protected final String indexedField;
  protected final String val;
  protected final BytesRef indexedBytes;

  public TotalTermFreqValueSource(
      String field, String val, String indexedField, BytesRef indexedBytes) {
    this.field = field;
    this.val = val;
    this.indexedField = indexedField;
    this.indexedBytes = indexedBytes;
  }

  public String name() {
    return "totaltermfreq";
  }

  @Override
  public String description() {
    return name() + '(' + field + ',' + val + ')';
  }

  @Override
  public FunctionValues getValues(Map<Object, Object> context, LeafReaderContext readerContext)
      throws IOException {
    return (FunctionValues) context.get(this);
  }

  @Override
  public void createWeight(Map<Object, Object> context, IndexSearcher searcher) throws IOException {
    long totalTermFreq = 0;
    for (LeafReaderContext readerContext : searcher.getTopReaderContext().leaves()) {
      long val = readerContext.reader().totalTermFreq(new Term(indexedField, indexedBytes));
      assert val != -1;
      totalTermFreq += val;
    }
    final long ttf = totalTermFreq;
    context.put(
        this,
        new LongDocValues(this) {
          @Override
          public long longVal(int doc) {
            return ttf;
          }
        });
  }

  @Override
  public int hashCode() {
    return getClass().hashCode() + indexedField.hashCode() * 29 + indexedBytes.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this.getClass() != o.getClass()) return false;
    TotalTermFreqValueSource other = (TotalTermFreqValueSource) o;
    return this.indexedField.equals(other.indexedField)
        && this.indexedBytes.equals(other.indexedBytes);
  }
}
