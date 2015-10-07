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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.OfflineSorter;
import org.apache.lucene.util.RamUsageEstimator;

final class OfflineWriter implements Writer {

  final Path tempFile;
  final OutputStreamDataOutput out;
  final long count;
  final int packedBytesLength;
  final int bytesPerDoc;
  private long countWritten;
  private boolean closed;

  public OfflineWriter(long count, int packedBytesLength) throws IOException {
    tempFile = Files.createTempFile(OfflineSorter.getDefaultTempDir(), "size" + count + ".", "");
    // nocommit cutover to Directory API
    out = new OutputStreamDataOutput(new BufferedOutputStream(Files.newOutputStream(tempFile)));
    this.packedBytesLength = packedBytesLength;
    bytesPerDoc = packedBytesLength + RamUsageEstimator.NUM_BYTES_LONG + RamUsageEstimator.NUM_BYTES_INT;
    this.count = count;
  }
    
  @Override
  public void append(byte[] packedValue, long ord, int docID) throws IOException {
    assert packedValue.length == packedBytesLength;
    out.writeBytes(packedValue, 0, packedValue.length);
    out.writeLong(ord);
    out.writeInt(docID);
    countWritten++;
  }

  @Override
  public Reader getReader(long start) throws IOException {
    assert closed;
    return new OfflineReader(tempFile, packedBytesLength, start, count-start);
  }

  @Override
  public void close() throws IOException {
    closed = true;
    out.close();
    if (count != countWritten) {
      throw new IllegalStateException("wrote " + countWritten + " values, but expected " + count);
    }
  }

  @Override
  public void destroy() throws IOException {
    IOUtils.rm(tempFile);
  }

  @Override
  public String toString() {
    return "OfflineWriter(count=" + count + " tempFile=" + tempFile + ")";
  }
}

