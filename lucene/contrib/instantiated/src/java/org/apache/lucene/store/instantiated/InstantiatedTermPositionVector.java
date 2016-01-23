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

import org.apache.lucene.index.TermPositionVector;
import org.apache.lucene.index.TermVectorOffsetInfo;

import java.io.Serializable;

/**
 * Extended vector space view of a document in an {@link InstantiatedIndexReader}.
 *
 * @see org.apache.lucene.index.TermPositionVector
 */
public class InstantiatedTermPositionVector
    extends InstantiatedTermFreqVector
    implements TermPositionVector, Serializable {

  private static final long serialVersionUID = 1l;

  public InstantiatedTermPositionVector(InstantiatedDocument document, String field) {
    super(document, field);
  }

  public int[] getTermPositions(int index) {
    return getTermDocumentInformations().get(index).getTermPositions();
  }

  public TermVectorOffsetInfo[] getOffsets(int index) {
    return getTermDocumentInformations().get(index).getTermOffsets();
  }

}
