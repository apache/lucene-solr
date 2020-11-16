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
  final BKDConfig config;
  long count;
  private boolean closed;
  final long expectedCount;

  /** Create a new writer with an unknown number of incoming points */
  public OfflinePointWriter(BKDConfig config, Directory tempDir, String tempFileNamePrefix,
                            String desc, long expectedCount) throws IOException {
    this.out = tempDir.createTempOutput(tempFileNamePrefix, "bkd_" + desc, IOContext.DEFAULT);
    this.name = out.getName();
    this.tempDir = tempDir;
    this.config = config;
    this.expectedCount = expectedCount;
  }

  @Override
  public void append(byte[] packedValue, int docID) throws IOException {
    assert closed == false : "Point writer is already closed";
    assert packedValue.length == config.packedBytesLength : "[packedValue] must have length [" + config.packedBytesLength + "] but was [" + packedValue.length + "]";
    out.writeBytes(packedValue, 0, packedValue.length);
    out.writeInt(docID);
    count++;
    assert expectedCount == 0 || count <= expectedCount:  "expectedCount=" + expectedCount + " vs count=" + count;
  }

  @Override
  public void append(PointValue pointValue) throws IOException {
    assert closed == false : "Point writer is already closed";
    BytesRef packedValueDocID = pointValue.packedValueDocIDBytes();
    assert packedValueDocID.length == config.bytesPerDoc : "[packedValue and docID] must have length [" + (config.bytesPerDoc) + "] but was [" + packedValueDocID.length + "]";
    out.writeBytes(packedValueDocID.bytes, packedValueDocID.offset, packedValueDocID.length);
    count++;
    assert expectedCount == 0 || count <= expectedCount : "expectedCount=" + expectedCount + " vs count=" + count;
  }

  @Override
  public PointReader getReader(long start, long length) throws IOException {
    byte[] buffer  = new byte[config.bytesPerDoc];
    return getReader(start, length,  buffer);
  }

  protected OfflinePointReader getReader(long start, long length, byte[] reusableBuffer) throws IOException {
    assert closed: "point writer is still open and trying to get a reader";
    assert start + length <= count: "start=" + start + " length=" + length + " count=" + count;
    assert expectedCount == 0 || count == expectedCount;
    return new OfflinePointReader(config, tempDir, name, start, length, reusableBuffer);
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
