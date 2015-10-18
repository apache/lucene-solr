package org.apache.lucene.util.bkd;

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

import java.io.EOFException;
import java.io.IOException;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.OfflineSorter;
import org.apache.lucene.util.RamUsageEstimator;

final class OfflinePointReader extends OfflineSorter.ByteSequencesReader implements PointReader {
  long countLeft;
  private final byte[] packedValue;
  private long ord;
  private int docID;
  final int bytesPerDoc;

  OfflinePointReader(Directory tempDir, String tempFileName, int packedBytesLength, long start, long length) throws IOException {
    this(tempDir.openInput(tempFileName, IOContext.READONCE), packedBytesLength, start, length);
  }

  OfflinePointReader(IndexInput in, int packedBytesLength, long start, long length) throws IOException {
    super(in);
    bytesPerDoc = packedBytesLength + RamUsageEstimator.NUM_BYTES_LONG + RamUsageEstimator.NUM_BYTES_INT;
    long seekFP = start * bytesPerDoc;
    in.seek(seekFP);
    this.countLeft = length;
    packedValue = new byte[packedBytesLength];
  }

  @Override
  public boolean next() throws IOException {
    if (countLeft >= 0) {
      if (countLeft == 0) {
        return false;
      }
      countLeft--;
    }
    try {
      in.readBytes(packedValue, 0, packedValue.length);
    } catch (EOFException eofe) {
      assert countLeft == -1;
      return false;
    }
    ord = in.readLong();
    docID = in.readInt();
    return true;
  }

  @Override
  public boolean read(BytesRefBuilder ref) throws IOException {
    ref.grow(bytesPerDoc);
    try {
      in.readBytes(ref.bytes(), 0, bytesPerDoc);
    } catch (EOFException eofe) {
      return false;
    }
    ref.setLength(bytesPerDoc);
    return true;
  }

  @Override
  public byte[] packedValue() {
    return packedValue;
  }

  @Override
  public long ord() {
    return ord;
  }

  @Override
  public int docID() {
    return docID;
  }

  @Override
  public void close() throws IOException {
    in.close();
  }
}

