package org.apache.lucene.search.join;

/**
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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;

/** Expert: creates a filter accepting all documents
 *  containing the provided term, disregarding deleted
 *  documents.  NOTE: this filter only works on readers
 *  whose sequential sub readers are SegmentReaders.
 *
 *  <p>Normally you should not use this class; the only
 *  known required case is when using BlockJoinQuery from
 *  contrib/join.
 * 
 *  @lucene.experimental */
public class RawTermFilter extends Filter {

  private final Term term;

  public RawTermFilter(Term term) {
    this.term = term;
  }

  @Override
  public DocIdSet getDocIdSet(final IndexReader reader) throws IOException {
    if (!(reader instanceof SegmentReader)) {
      throw new IllegalArgumentException("this filter can only work with SegmentReaders (got: " + reader + ")");
    }

    final SegmentReader segReader = (SegmentReader) reader;

    return new DocIdSet() {

      @Override
      public DocIdSetIterator iterator() throws IOException {
        final TermDocs td = segReader.rawTermDocs(term);
        return new DocIdSetIterator() {

          private int docID;

          @Override
          public int nextDoc() throws IOException {
            if (td.next()) {
              docID = td.doc();
            } else {
              docID = NO_MORE_DOCS;
            }
            return docID;
          }

          @Override
          public int docID() {
            return docID;
          }

          @Override
          public int advance(int target) throws IOException {
            if (td.skipTo(target)) {
              docID = td.doc();
            } else {
              docID = NO_MORE_DOCS;
            }
            return docID;
          }
        };
      }
    };
  }
}
