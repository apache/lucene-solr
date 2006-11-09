package org.apache.lucene.index;

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

import org.apache.lucene.store.IndexInput;

final class SegmentTermPositions
extends SegmentTermDocs implements TermPositions {
  private IndexInput proxStream;
  private int proxCount;
  private int position;
  
  // these variables are being used to remember information
  // for a lazy skip
  private long lazySkipPointer = 0;
  private int lazySkipDocCount = 0;
  
  SegmentTermPositions(SegmentReader p) {
    super(p);
    this.proxStream = (IndexInput)parent.proxStream.clone();
  }

  final void seek(TermInfo ti) throws IOException {
    super.seek(ti);
    if (ti != null)
      lazySkipPointer = ti.proxPointer;
    
    lazySkipDocCount = 0;
    proxCount = 0;
  }

  public final void close() throws IOException {
    super.close();
    proxStream.close();
  }

  public final int nextPosition() throws IOException {
    // perform lazy skips if neccessary
    lazySkip();
    proxCount--;
    return position += proxStream.readVInt();
  }

  protected final void skippingDoc() throws IOException {
    // we remember to skip the remaining positions of the current
    // document lazily
    lazySkipDocCount += freq;
  }

  public final boolean next() throws IOException {
    // we remember to skip a document lazily
    lazySkipDocCount += proxCount;
    
    if (super.next()) {               // run super
      proxCount = freq;               // note frequency
      position = 0;               // reset position
      return true;
    }
    return false;
  }

  public final int read(final int[] docs, final int[] freqs) {
    throw new UnsupportedOperationException("TermPositions does not support processing multiple documents in one call. Use TermDocs instead.");
  }


  /** Called by super.skipTo(). */
  protected void skipProx(long proxPointer) throws IOException {
    // we save the pointer, we might have to skip there lazily
    lazySkipPointer = proxPointer;
    lazySkipDocCount = 0;
    proxCount = 0;
  }

  private void skipPositions(int n) throws IOException {
    for (int f = n; f > 0; f--)         // skip unread positions
      proxStream.readVInt();
  }

  // It is not always neccessary to move the prox pointer
  // to a new document after the freq pointer has been moved.
  // Consider for example a phrase query with two terms:
  // the freq pointer for term 1 has to move to document x
  // to answer the question if the term occurs in that document. But
  // only if term 2 also matches document x, the positions have to be
  // read to figure out if term 1 and term 2 appear next
  // to each other in document x and thus satisfy the query.
  // So we move the prox pointer lazily to the document
  // as soon as positions are requested.
  private void lazySkip() throws IOException {
    if (lazySkipPointer != 0) {
      proxStream.seek(lazySkipPointer);
      lazySkipPointer = 0;
    }
     
    if (lazySkipDocCount != 0) {
      skipPositions(lazySkipDocCount);
      lazySkipDocCount = 0;
    }
  }

}
