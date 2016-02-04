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
package org.apache.lucene.util.bkd;


import java.io.EOFException;
import java.io.IOException;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.RamUsageEstimator;

/** Reads points from disk in a fixed-with format, previously written with {@link OfflinePointWriter}. */
final class OfflinePointReader implements PointReader {
  long countLeft;
  private final IndexInput in;
  private final byte[] packedValue;
  private long ord;
  private int docID;
  final int bytesPerDoc;

  OfflinePointReader(Directory tempDir, String tempFileName, int packedBytesLength, long start, long length) throws IOException {
    this(tempDir.openInput(tempFileName, IOContext.READONCE), packedBytesLength, start, length);
  }

  private OfflinePointReader(IndexInput in, int packedBytesLength, long start, long length) throws IOException {
    this.in = in;
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

