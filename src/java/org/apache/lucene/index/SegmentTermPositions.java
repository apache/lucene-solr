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

import org.apache.lucene.store.IndexInput;

import java.io.IOException;

final class SegmentTermPositions
extends SegmentTermDocs implements TermPositions {
  private IndexInput proxStream;
  private int proxCount;
  private int position;
  
  // the current payload length
  private int payloadLength;
  // indicates whether the payload of the currend position has
  // been read from the proxStream yet
  private boolean needToLoadPayload;
  
  // these variables are being used to remember information
  // for a lazy skip
  private long lazySkipPointer = -1;
  private int lazySkipProxCount = 0;
  
  SegmentTermPositions(SegmentReader p) {
    super(p);
    this.proxStream = null;  // the proxStream will be cloned lazily when nextPosition() is called for the first time
  }

  final void seek(TermInfo ti, Term term) throws IOException {
    super.seek(ti, term);
    if (ti != null)
      lazySkipPointer = ti.proxPointer;
    
    lazySkipProxCount = 0;
    proxCount = 0;
    payloadLength = 0;
    needToLoadPayload = false;
  }

  public final void close() throws IOException {
    super.close();
    if (proxStream != null) proxStream.close();
  }

  public final int nextPosition() throws IOException {
    if (currentFieldOmitTf)
      // This field does not store term freq, positions, payloads
      return 0;
    // perform lazy skips if neccessary
    lazySkip();
    proxCount--;
    return position += readDeltaPosition();
  }

  private final int readDeltaPosition() throws IOException {
    int delta = proxStream.readVInt();
    if (currentFieldStoresPayloads) {
      // if the current field stores payloads then
      // the position delta is shifted one bit to the left.
      // if the LSB is set, then we have to read the current
      // payload length
      if ((delta & 1) != 0) {
        payloadLength = proxStream.readVInt();
      } 
      delta >>>= 1;
      needToLoadPayload = true;
    }
    return delta;
  }
  
  protected final void skippingDoc() throws IOException {
    // we remember to skip a document lazily
    lazySkipProxCount += freq;
  }

  public final boolean next() throws IOException {
    // we remember to skip the remaining positions of the current
    // document lazily
    lazySkipProxCount += proxCount;
    
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
  protected void skipProx(long proxPointer, int payloadLength) throws IOException {
    // we save the pointer, we might have to skip there lazily
    lazySkipPointer = proxPointer;
    lazySkipProxCount = 0;
    proxCount = 0;
    this.payloadLength = payloadLength;
    needToLoadPayload = false;
  }

  private void skipPositions(int n) throws IOException {
    assert !currentFieldOmitTf;
    for (int f = n; f > 0; f--) {        // skip unread positions
      readDeltaPosition();
      skipPayload();
    }      
  }
  
  private void skipPayload() throws IOException {
    if (needToLoadPayload && payloadLength > 0) {
      proxStream.seek(proxStream.getFilePointer() + payloadLength);
    }
    needToLoadPayload = false;
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
    if (proxStream == null) {
      // clone lazily
      proxStream = (IndexInput)parent.proxStream.clone();
    }
    
    // we might have to skip the current payload
    // if it was not read yet
    skipPayload();
      
    if (lazySkipPointer != -1) {
      proxStream.seek(lazySkipPointer);
      lazySkipPointer = -1;
    }
     
    if (lazySkipProxCount != 0) {
      skipPositions(lazySkipProxCount);
      lazySkipProxCount = 0;
    }
  }
  
  public int getPayloadLength() {
    return payloadLength;
  }

  public byte[] getPayload(byte[] data, int offset) throws IOException {
    if (!needToLoadPayload) {
      throw new IOException("Payload cannot be loaded more than once for the same term position.");
    }

    // read payloads lazily
    byte[] retArray;
    int retOffset;
    if (data == null || data.length - offset < payloadLength) {
      // the array is too small to store the payload data,
      // so we allocate a new one
      retArray = new byte[payloadLength];
      retOffset = 0;
    } else {
      retArray = data;
      retOffset = offset;
    }
    proxStream.readBytes(retArray, retOffset, payloadLength);
    needToLoadPayload = false;
    return retArray;
  }

  public boolean isPayloadAvailable() {
    return needToLoadPayload && payloadLength > 0;
  }

}
