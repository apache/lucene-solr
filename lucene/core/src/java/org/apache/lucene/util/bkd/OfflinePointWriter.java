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

import java.io.IOException;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;

/**
 * Writes points to disk in a fixed-with format.
 *
 * @lucene.internal
 * */
public final class OfflinePointWriter implements PointWriter {

  final Directory tempDir;
  public final IndexOutput out;
  public final String name;
  final int packedBytesLength;
  long count;
  private boolean closed;
  final long expectedCount;

  /** Create a new writer with an unknown number of incoming points */
  public OfflinePointWriter(Directory tempDir, String tempFileNamePrefix, int packedBytesLength,
                            String desc, long expectedCount) throws IOException {
    this.out = tempDir.createTempOutput(tempFileNamePrefix, "bkd_" + desc, IOContext.DEFAULT);
    this.name = out.getName();
    this.tempDir = tempDir;
    this.packedBytesLength = packedBytesLength;

    this.expectedCount = expectedCount;
  }
    
  @Override
  public void append(byte[] packedValue, int docID) throws IOException {
    assert packedValue.length == packedBytesLength;
    out.writeBytes(packedValue, 0, packedValue.length);
    out.writeInt(docID);
    count++;
    assert expectedCount == 0 || count <= expectedCount;
  }

  @Override
  public void append(BytesRef packedValue, int docID) throws IOException {
    assert packedValue.length == packedBytesLength;
    out.writeBytes(packedValue.bytes, packedValue.offset, packedValue.length);
    out.writeInt(docID);
    count++;
    assert expectedCount == 0 || count <= expectedCount;
  }

  @Override
  public PointReader getReader(long start, long length) throws IOException {
    byte[] buffer  = new byte[packedBytesLength + Integer.BYTES];
    return getReader(start, length,  buffer);
  }

  protected OfflinePointReader getReader(long start, long length, byte[] reusableBuffer) throws IOException {
    assert closed;
    assert start + length <= count: "start=" + start + " length=" + length + " count=" + count;
    assert expectedCount == 0 || count == expectedCount;
    return new OfflinePointReader(tempDir, name, packedBytesLength, start, length, reusableBuffer);
  }

  @Override
  public long count() {
    return count;
  }

  @Override
  public void close() throws IOException {
    if (closed == false) {
      try {
        CodecUtil.writeFooter(out);
      } finally {
        out.close();
        closed = true;
      }
    }
  }

  @Override
  public void destroy() throws IOException {
    tempDir.deleteFile(name);
  }

  @Override
  public String toString() {
    return "OfflinePointWriter(count=" + count + " tempFileName=" + name + ")";
  }
}
