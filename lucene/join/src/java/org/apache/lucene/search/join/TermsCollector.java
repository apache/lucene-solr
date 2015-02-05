package org.apache.lucene.search.join;

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

import java.io.IOException;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;

/**
 * A collector that collects all terms from a specified field matching the query.
 *
 * @lucene.experimental
 */
abstract class TermsCollector extends SimpleCollector {

  final String field;
  final BytesRefHash collectorTerms = new BytesRefHash();

  TermsCollector(String field) {
    this.field = field;
  }

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
  static TermsCollector create(String field, boolean multipleValuesPerDocument) {
    return multipleValuesPerDocument ? new MV(field) : new SV(field);
  }

  // impl that works with multiple values per document
  static class MV extends TermsCollector {
    final BytesRef scratch = new BytesRef();
    private SortedSetDocValues docTermOrds;

    MV(String field) {
      super(field);
    }

    @Override
    public void collect(int doc) throws IOException {
      docTermOrds.setDocument(doc);
      long ord;
      while ((ord = docTermOrds.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
        final BytesRef term = docTermOrds.lookupOrd(ord);
        collectorTerms.add(term);
      }
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      docTermOrds = DocValues.getSortedSet(context.reader(), field);
    }
  }

  // impl that works with single value per document
  static class SV extends TermsCollector {

    final BytesRef spare = new BytesRef();
    private BinaryDocValues fromDocTerms;

    SV(String field) {
      super(field);
    }

    @Override
    public void collect(int doc) throws IOException {
      final BytesRef term = fromDocTerms.get(doc);
      collectorTerms.add(term);
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      fromDocTerms = DocValues.getBinary(context.reader(), field);
    }
  }

  @Override
  public boolean needsScores() {
    return false;
  }
}
