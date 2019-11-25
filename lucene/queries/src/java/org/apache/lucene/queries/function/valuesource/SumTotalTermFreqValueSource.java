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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.LongDocValues;
import org.apache.lucene.search.IndexSearcher;

import java.io.IOException;
import java.util.Map;

/**
 * <code>SumTotalTermFreqValueSource</code> returns the number of tokens.
 * (sum of term freqs across all documents, across all terms).
 * @lucene.internal
 */
public class SumTotalTermFreqValueSource extends ValueSource {
  protected final String indexedField;

  public SumTotalTermFreqValueSource(String indexedField) {
    this.indexedField = indexedField;
  }

  public String name() {
    return "sumtotaltermfreq";
  }

  @Override
  public String description() {
    return name() + '(' + indexedField + ')';
  }

  @SuppressWarnings("rawtypes")
  @Override
  public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
    return (FunctionValues)context.get(this);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public void createWeight(Map context, IndexSearcher searcher) throws IOException {
    long sumTotalTermFreq = 0;
    for (LeafReaderContext readerContext : searcher.getTopReaderContext().leaves()) {
      Terms terms = readerContext.reader().terms(indexedField);
      if (terms == null) continue;
      long v = terms.getSumTotalTermFreq();
      assert v != -1;
      sumTotalTermFreq += v;
    }
    final long ttf = sumTotalTermFreq;
    context.put(this, new LongDocValues(this) {
      @Override
      public long longVal(int doc) {
        return ttf;
      }
    });
  }

  @Override
  public int hashCode() {
    return getClass().hashCode() + indexedField.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this.getClass() != o.getClass()) return false;
    SumTotalTermFreqValueSource other = (SumTotalTermFreqValueSource)o;
    return this.indexedField.equals(other.indexedField);
  }
}
