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
package org.apache.lucene.search.join;

import java.io.IOException;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;

/**
 * A collector that collects all terms from a specified field matching the query.
 *
 * @lucene.experimental
 */
abstract class TermsCollector<DV> extends DocValuesTermsCollector<DV> {

  TermsCollector(Function<DV> docValuesCall) {
    super(docValuesCall);
  }

  final BytesRefHash collectorTerms = new BytesRefHash();

  public BytesRefHash getCollectorTerms() {
    return collectorTerms;
  }

  
  /**
   * Chooses the right {@link TermsCollector} implementation.
   *
   * @param field                     The field to collect terms for
   * @param multipleValuesPerDocument Whether the field to collect terms for has multiple values per document.
   * @return a {@link TermsCollector} instance
   */
  static TermsCollector<?> create(String field, boolean multipleValuesPerDocument) {
    return multipleValuesPerDocument 
        ? new MV(sortedSetDocValues(field))
        : new SV(binaryDocValues(field));
  }
  
  // impl that works with multiple values per document
  static class MV extends TermsCollector<SortedSetDocValues> {
    
    MV(Function<SortedSetDocValues> docValuesCall) {
      super(docValuesCall);
    }

    @Override
    public void collect(int doc) throws IOException {
      long ord;
      docValues.setDocument(doc);
      while ((ord = docValues.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
        final BytesRef term = docValues.lookupOrd(ord);
        collectorTerms.add(term);
      }
    }
  }

  // impl that works with single value per document
  static class SV extends TermsCollector<BinaryDocValues> {

    SV(Function<BinaryDocValues> docValuesCall) {
      super(docValuesCall);
    }

    @Override
    public void collect(int doc) throws IOException {
      final BytesRef term = docValues.get(doc);
      collectorTerms.add(term);
    }
  }

  @Override
  public boolean needsScores() {
    return false;
  }
}
