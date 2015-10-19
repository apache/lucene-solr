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

import java.io.IOException;


import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.RamUsageEstimator;

/** Writes points to disk in a fixed-with format. */
final class OfflinePointWriter implements PointWriter {

  final Directory tempDir;
  final IndexOutput out;
  final int packedBytesLength;
  final int bytesPerDoc;
  private long count;
  private boolean closed;

  public OfflinePointWriter(Directory tempDir, String tempFileNamePrefix, int packedBytesLength) throws IOException {
    this.out = tempDir.createTempOutput(tempFileNamePrefix, "bkd", IOContext.DEFAULT);
    this.tempDir = tempDir;
    this.packedBytesLength = packedBytesLength;
    bytesPerDoc = packedBytesLength + RamUsageEstimator.NUM_BYTES_LONG + RamUsageEstimator.NUM_BYTES_INT;
  }

  /** Initializes on an already written/closed file, just so consumers can use {@link #getReader} to read the file. */
  public OfflinePointWriter(Directory tempDir, IndexOutput out, int packedBytesLength, long count) {
    this.out = out;
    this.tempDir = tempDir;
    this.packedBytesLength = packedBytesLength;
    bytesPerDoc = packedBytesLength + RamUsageEstimator.NUM_BYTES_LONG + RamUsageEstimator.NUM_BYTES_INT;
    this.count = count;
    closed = true;
  }
    
  @Override
  public void append(byte[] packedValue, long ord, int docID) throws IOException {
    assert packedValue.length == packedBytesLength;
    out.writeBytes(packedValue, 0, packedValue.length);
    out.writeLong(ord);
    out.writeInt(docID);
    count++;
  }

  @Override
  public PointReader getReader(long start) throws IOException {
    assert closed;
    return new OfflinePointReader(tempDir, out.getName(), packedBytesLength, start, count-start);
  }

  @Override
  public void close() throws IOException {
    out.close();
    closed = true;
  }

  @Override
  public void destroy() throws IOException {
    tempDir.deleteFile(out.getName());
  }

  @Override
  public String toString() {
    return "OfflinePointWriter(count=" + count + " tempFileName=" + out.getName() + ")";
  }
}

