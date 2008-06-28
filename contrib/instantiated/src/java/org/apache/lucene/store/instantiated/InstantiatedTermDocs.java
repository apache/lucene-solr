package org.apache.lucene.store.instantiated;

/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;

/**
 * A {@link org.apache.lucene.index.TermDocs} navigating an {@link InstantiatedIndexReader}.
 */
public class InstantiatedTermDocs
    implements TermDocs {

  private final InstantiatedIndexReader reader;

  public InstantiatedTermDocs(InstantiatedIndexReader reader) {
    this.reader = reader;
  }

  private int currentDocumentIndex;
  protected InstantiatedTermDocumentInformation currentDocumentInformation;
  protected InstantiatedTerm currentTerm;


  public void seek(Term term) {
    currentTerm = reader.getIndex().findTerm(term);
    currentDocumentIndex = -1;
  }

  public void seek(org.apache.lucene.index.TermEnum termEnum) {
    seek(termEnum.term());
  }


  public int doc() {
    return currentDocumentInformation.getDocument().getDocumentNumber();
  }

  public int freq() {
    return currentDocumentInformation.getTermPositions().length;
  }


  public boolean next() {
    if (currentTerm != null) {
      currentDocumentIndex++;
      if (currentDocumentIndex < currentTerm.getAssociatedDocuments().length) {
        currentDocumentInformation = currentTerm.getAssociatedDocuments()[currentDocumentIndex];
        if (reader.hasDeletions() && reader.isDeleted(currentDocumentInformation.getDocument().getDocumentNumber())) {
          return next();
        } else {
          return true;
        }
      }
    }
    return false;
  }


  public int read(int[] docs, int[] freqs) {
    int i;
    for (i = 0; i < docs.length; i++) {
      if (!next()) {
        break;
      }
      docs[i] = doc();
      freqs[i] = freq();
    }
    return i;
  }

  /**
   * Skips entries to the first beyond the current whose document number is
   * greater than or equal to <i>target</i>. <p>Returns true if there is such
   * an entry.  <p>Behaves as if written: <pre>
   *   boolean skipTo(int target) {
   *     do {
   *       if (!next())
   * 	     return false;
   *     } while (target > doc());
   *     return true;
   *   }
   * </pre>
   * This implementation is considerably more efficient than that.
   *
   */
  public boolean skipTo(int target) {
    if (currentTerm == null) {
      return false;
    }
    
    if (currentDocumentIndex >= target) {
      return next();
    }

    int startOffset = currentDocumentIndex >= 0 ? currentDocumentIndex : 0;
    int pos = currentTerm.seekCeilingDocumentInformationIndex(target, startOffset);

    if (pos == -1) {
      return false;
    }

    currentDocumentInformation = currentTerm.getAssociatedDocuments()[pos];
    currentDocumentIndex = pos;
    if (reader.hasDeletions() && reader.isDeleted(currentDocumentInformation.getDocument().getDocumentNumber())) {
      return next();
    } else {
      return true;
    }
  }

  /**
   * Does nothing
   */
  public void close() {
  }
}
