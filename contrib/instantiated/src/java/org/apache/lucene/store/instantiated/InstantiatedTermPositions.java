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

import org.apache.lucene.index.TermPositions;

import java.io.IOException;

/**
 * A {@link org.apache.lucene.index.TermPositions} navigating an {@link InstantiatedIndexReader}.
 */
public class InstantiatedTermPositions
    extends InstantiatedTermDocs
    implements TermPositions {

  public int getPayloadLength() {
    return currentDocumentInformation.getPayloads()[currentTermPositionIndex].length;
  }

  public byte[] getPayload(byte[] data, int offset) throws IOException {
    byte[] payloads = currentDocumentInformation.getPayloads()[currentTermPositionIndex];

    // read payloads lazily
    if (data == null || data.length - offset < getPayloadLength()) {
      // the array is too small to store the payload data,
      return payloads;
    } else {
      System.arraycopy(payloads, 0, data, offset, payloads.length);
      return data;
    }
  }

  public boolean isPayloadAvailable() {
    return currentDocumentInformation.getPayloads()[currentTermPositionIndex] != null;
  }

  public InstantiatedTermPositions(InstantiatedIndexReader reader) {
    super(reader);
  }

  /**
   * Returns next position in the current document.  It is an error to call
   * this more than {@link #freq()} times
   * without calling {@link #next()}<p> This is
   * invalid until {@link #next()} is called for
   * the first time.
   */
  public int nextPosition() {
    currentTermPositionIndex++;
    // if you get an array out of index exception here,
    // it might be due to currentDocumentInformation.getIndexFromTerm not beeing set!!
    return currentDocumentInformation.getTermPositions()[currentTermPositionIndex];
  }

  private int currentTermPositionIndex;

  /**
   * Moves to the next pair in the enumeration.
   * <p> Returns true if there is such a next pair in the enumeration.
   */
  @Override
  public boolean next() {
    currentTermPositionIndex = -1;
    return super.next();
  }

  /**
   * Skips entries to the first beyond the current whose document number is
   * greater than or equal to <currentTermPositionIndex>target</currentTermPositionIndex>. <p>Returns true iff there is such
   * an entry.  <p>Behaves as if written: <pre>
   *   boolean skipTo(int target) {
   *     do {
   *       if (!next())
   * 	     return false;
   *     } while (target > doc());
   *     return true;
   *   }
   * </pre>
   * Some implementations are considerably more efficient than that.
   */
  @Override
  public boolean skipTo(int target) {
    currentTermPositionIndex = -1;
    return super.skipTo(target);
  }
}
