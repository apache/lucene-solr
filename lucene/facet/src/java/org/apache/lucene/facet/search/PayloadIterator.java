package org.apache.lucene.facet.search;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;

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

/**
 * A utility class for iterating through a posting list of a given term and
 * retrieving the payload of the first position in every document. For
 * efficiency, this class does not check if documents passed to
 * {@link #getPayload(int)} are deleted, since it is usually used to iterate on
 * payloads of documents that matched a query. If you need to skip over deleted
 * documents, you should do so before calling {@link #getPayload(int)}.
 * 
 * @lucene.experimental
 */
public class PayloadIterator {

  private TermsEnum reuseTE;
  private DocsAndPositionsEnum dpe;
  private boolean hasMore;
  private int curDocID;
  
  private final Term term;

  public PayloadIterator(Term term) throws IOException {
    this.term = term;
  }

  /**
   * Sets the {@link AtomicReaderContext} for which {@link #getPayload(int)}
   * calls will be made. Returns true iff this reader has payload for any of the
   * documents belonging to the {@link Term} given to the constructor.
   */
  public boolean setNextReader(AtomicReaderContext context) throws IOException {
    hasMore = false;
    Fields fields = context.reader().fields();
    if (fields != null) {
      Terms terms = fields.terms(term.field());
      if (terms != null) {
        reuseTE = terms.iterator(reuseTE);
        if (reuseTE.seekExact(term.bytes(), true)) {
          // this class is usually used to iterate on whatever a Query matched
          // if it didn't match deleted documents, we won't receive them. if it
          // did, we should iterate on them too, therefore we pass liveDocs=null
          dpe = reuseTE.docsAndPositions(null, dpe, DocsAndPositionsEnum.FLAG_PAYLOADS);
          if (dpe != null && (curDocID = dpe.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            hasMore = true;
          }
        }
      }
    }
    return hasMore;
  }
  
  /**
   * Returns the {@link BytesRef payload} of the given document, or {@code null}
   * if the document does not exist, there are no more documents in the posting
   * list, or the document exists but has not payload. The given document IDs
   * are treated as local to the reader given to
   * {@link #setNextReader(AtomicReaderContext)}.
   */
  public BytesRef getPayload(int docID) throws IOException {
    if (!hasMore) {
      return null;
    }

    if (curDocID > docID) {
      // document does not exist
      return null;
    }
    
    if (curDocID < docID) {
      curDocID = dpe.advance(docID);
      if (curDocID != docID) { // requested document does not have a payload
        if (curDocID == DocIdSetIterator.NO_MORE_DOCS) { // no more docs in this reader
          hasMore = false;
        }
        return null;
      }
    }

    // we're on the document
    assert dpe.freq() == 1 : "expecting freq=1 (got " + dpe.freq() + ") term=" + term + " doc=" + curDocID;
    int pos = dpe.nextPosition();
    assert pos != -1 : "no positions for term=" + term + " doc=" + curDocID;
    return dpe.getPayload();
  }
  
}
