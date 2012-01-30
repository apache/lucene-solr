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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocTermOrds;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;

import java.io.IOException;

/**
 * A collector that collects all terms from a specified field matching the query.
 *
 * @lucene.experimental
 */
abstract class TermsCollector extends Collector {

  final String field;
  final BytesRefHash collectorTerms = new BytesRefHash();

  TermsCollector(String field) {
    this.field = field;
  }

  public BytesRefHash getCollectorTerms() {
    return collectorTerms;
  }

  public void setScorer(Scorer scorer) throws IOException {
  }

  public boolean acceptsDocsOutOfOrder() {
    return true;
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

    private DocTermOrds docTermOrds;
    private TermsEnum docTermsEnum;
    private DocTermOrds.TermOrdsIterator reuse;

    MV(String field) {
      super(field);
    }

    public void collect(int doc) throws IOException {
      reuse = docTermOrds.lookup(doc, reuse);
      int[] buffer = new int[5];

      int chunk;
      do {
        chunk = reuse.read(buffer);
        if (chunk == 0) {
          return;
        }

        for (int idx = 0; idx < chunk; idx++) {
          int key = buffer[idx];
          docTermsEnum.seekExact((long) key);
          collectorTerms.add(docTermsEnum.term());
        }
      } while (chunk >= buffer.length);
    }

    public void setNextReader(AtomicReaderContext context) throws IOException {
      docTermOrds = FieldCache.DEFAULT.getDocTermOrds(context.reader(), field);
      docTermsEnum = docTermOrds.getOrdTermsEnum(context.reader());
      reuse = null; // LUCENE-3377 needs to be fixed first then this statement can be removed...
    }
  }

  // impl that works with single value per document
  static class SV extends TermsCollector {

    final BytesRef spare = new BytesRef();
    private FieldCache.DocTerms fromDocTerms;

    SV(String field) {
      super(field);
    }

    public void collect(int doc) throws IOException {
      collectorTerms.add(fromDocTerms.getTerm(doc, spare));
    }

    public void setNextReader(AtomicReaderContext context) throws IOException {
      fromDocTerms = FieldCache.DEFAULT.getTerms(context.reader(), field);
    }
  }

}
