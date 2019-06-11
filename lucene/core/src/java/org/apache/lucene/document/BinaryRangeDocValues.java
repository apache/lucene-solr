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

package org.apache.lucene.document;

import java.io.IOException;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.BytesRef;

class BinaryRangeDocValues extends BinaryDocValues {
  private final BinaryDocValues in;
  private byte[] packedValue;
  private final int numDims;
  private final int numBytesPerDimension;
  private int docID = -1;

  BinaryRangeDocValues(BinaryDocValues in, int numDims, int numBytesPerDimension) {
    this.in = in;
    this.numBytesPerDimension = numBytesPerDimension;
    this.numDims = numDims;
    this.packedValue = new byte[2 * numDims * numBytesPerDimension];
  }

  @Override
  public int nextDoc() throws IOException {
    docID = in.nextDoc();

    if (docID != NO_MORE_DOCS) {
      decodeRanges();
    }

    return docID;
  }

  @Override
  public int docID() {
    return in.docID();
  }

  @Override
  public long cost() {
    return in.cost();
  }

  @Override
  public int advance(int target) throws IOException {
    int res = in.advance(target);
    if (res != NO_MORE_DOCS) {
      decodeRanges();
    }

    return res;
  }

  @Override
  public boolean advanceExact(int target) throws IOException {
    boolean res = in.advanceExact(target);
    if (res) {
      decodeRanges();
    }

    return res;
  }

  @Override
  public BytesRef binaryValue() throws IOException {
    return in.binaryValue();
  }

  public byte[] getPackedValue() {
    return packedValue;
  }

  private void decodeRanges() throws IOException {
    BytesRef bytesRef = in.binaryValue();

    // We reuse the existing allocated memory for packed values since all docvalues in this iterator
    // should be exactly same in indexed structure, hence the byte representations in length should be identical
    System.arraycopy(bytesRef.bytes, bytesRef.offset, packedValue, 0, 2 * numDims * numBytesPerDimension);
  }
}
