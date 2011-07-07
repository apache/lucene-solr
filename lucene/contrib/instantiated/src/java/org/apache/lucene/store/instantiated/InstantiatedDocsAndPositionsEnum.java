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

import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

public class InstantiatedDocsAndPositionsEnum extends DocsAndPositionsEnum {
  private int upto;
  private int posUpto;
  private Bits liveDocs;
  private InstantiatedTerm term;
  protected InstantiatedTermDocumentInformation currentDoc;
  private final BytesRef payload = new BytesRef();

  public InstantiatedDocsAndPositionsEnum reset(Bits liveDocs, InstantiatedTerm term) {
    this.liveDocs = liveDocs;
    this.term = term;
    upto = -1;
    return this;
  }

  @Override
  public int docID() {
    return currentDoc.getDocument().getDocumentNumber();
  }

  @Override
  public int nextDoc() {
    upto++;
    if (upto >= term.getAssociatedDocuments().length) {
      return NO_MORE_DOCS;
    } else {
      currentDoc = term.getAssociatedDocuments()[upto];
      if (liveDocs == null || liveDocs.get(currentDoc.getDocument().getDocumentNumber())) {
        posUpto = -1;
        return docID();
      } else {
        return nextDoc();
      }
    }
  }

  @Override
  public int advance(int target) {
    if (currentDoc.getDocument().getDocumentNumber() >= target) {
      return nextDoc();
    }

    int startOffset = upto >= 0 ? upto : 0;
    upto = term.seekCeilingDocumentInformationIndex(target, startOffset);
    if (upto == -1) {
      return NO_MORE_DOCS;
    }
    currentDoc = term.getAssociatedDocuments()[upto];

    if (liveDocs != null && !liveDocs.get(currentDoc.getDocument().getDocumentNumber())) {
      return nextDoc();
    } else {
      posUpto = -1;
      return docID();
    }
  }

  @Override
  public int freq() {
    return currentDoc.getTermPositions().length;
  }
  
  @Override
  public int nextPosition() {
    return currentDoc.getTermPositions()[++posUpto];
  }

  @Override
  public boolean hasPayload() {
    return currentDoc.getPayloads()[posUpto] != null;
  }

  @Override
  public BytesRef getPayload() {
    payload.bytes = currentDoc.getPayloads()[posUpto];
    payload.length = payload.bytes.length;
    return payload;
  }
}
