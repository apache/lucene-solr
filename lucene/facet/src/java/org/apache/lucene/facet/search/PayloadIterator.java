package org.apache.lucene.facet.search;

import java.io.IOException;
import java.util.Iterator;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
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
 * {@link #setdoc(int)} are deleted, since it is usually used to iterate on
 * payloads of documents that matched a query. If you need to skip over deleted
 * documents, you should do so before calling {@link #setdoc(int)}.
 * 
 * @lucene.experimental
 */
public class PayloadIterator {

  protected BytesRef data;

  private TermsEnum reuseTE;
  private DocsAndPositionsEnum currentDPE;
  private boolean hasMore;
  private int curDocID, curDocBase;
  
  private final Iterator<AtomicReaderContext> leaves;
  private final Term term;

  public PayloadIterator(IndexReader indexReader, Term term) throws IOException {
    leaves = indexReader.leaves().iterator();
    this.term = term;
  }

  private void nextSegment() throws IOException {
    hasMore = false;
    while (leaves.hasNext()) {
      AtomicReaderContext ctx = leaves.next();
      curDocBase = ctx.docBase;
      Fields fields = ctx.reader().fields();
      if (fields != null) {
        Terms terms = fields.terms(term.field());
        if (terms != null) {
          reuseTE = terms.iterator(reuseTE);
          if (reuseTE.seekExact(term.bytes(), true)) {
            // this class is usually used to iterate on whatever a Query matched
            // if it didn't match deleted documents, we won't receive them. if it
            // did, we should iterate on them too, therefore we pass liveDocs=null
            currentDPE = reuseTE.docsAndPositions(null, currentDPE, DocsAndPositionsEnum.FLAG_PAYLOADS);
            if (currentDPE != null && (curDocID = currentDPE.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
              hasMore = true;
              break;
            }
          }
        }
      }
    }
  }
  
  /**
   * Initialize the iterator. Should be done before the first call to
   * {@link #setdoc(int)}. Returns {@code false} if no category list is found,
   * or the category list has no documents.
   */
  public boolean init() throws IOException {
    nextSegment();
    return hasMore;
  }

  /**
   * Skip forward to document docId. Return true if this document exists and
   * has any payload.
   * <P>
   * Users should call this method with increasing docIds, and implementations
   * can assume that this is the case.
   */
  public boolean setdoc(int docId) throws IOException {
    if (!hasMore) {
      return false;
    }

    // re-basing docId->localDocID is done fewer times than currentDoc->globalDoc
    int localDocID = docId - curDocBase;
    
    if (curDocID > localDocID) {
      // document does not exist
      return false;
    }
    
    if (curDocID < localDocID) {
      // look for the document either in that segment, or others
      while (hasMore && (curDocID = currentDPE.advance(localDocID)) == DocIdSetIterator.NO_MORE_DOCS) {
        nextSegment(); // also updates curDocID
        localDocID = docId - curDocBase;
        // nextSegment advances to nextDoc, so check if we still need to advance
        if (curDocID >= localDocID) {
          break;
        }
      }
      
      // we break from the above loop when:
      // 1. we iterated over all segments (hasMore=false)
      // 2. current segment advanced to a doc, either requested or higher
      if (!hasMore || curDocID != localDocID) {
        return false;
      }
    }

    // we're on the document
    assert currentDPE.freq() == 1 : "expecting freq=1 (got " + currentDPE.freq() + ") term=" + term + " doc=" + (curDocID + curDocBase);
    int pos = currentDPE.nextPosition();
    assert pos != -1 : "no positions for term=" + term + " doc=" + (curDocID + curDocBase);
    data = currentDPE.getPayload();
    return data != null;
  }
  
  public BytesRef getPayload() {
    return data;
  }

}
