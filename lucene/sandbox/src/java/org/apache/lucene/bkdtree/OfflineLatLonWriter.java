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
package org.apache.lucene.bkdtree;

import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.OfflineSorter;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

final class OfflineLatLonWriter implements LatLonWriter {

  final Path tempFile;
  final byte[] scratchBytes = new byte[BKDTreeWriter.BYTES_PER_DOC];
  final ByteArrayDataOutput scratchBytesOutput = new ByteArrayDataOutput(scratchBytes);      
  final OutputStreamDataOutput out;
  final long count;
  private long countWritten;
  private boolean closed;

  public OfflineLatLonWriter(long count) throws IOException {
    tempFile = Files.createTempFile(OfflineSorter.getDefaultTempDir(), "size" + count + ".", "");
    out = new OutputStreamDataOutput(new BufferedOutputStream(Files.newOutputStream(tempFile)));
    this.count = count;
  }

  @Override
  public void append(int latEnc, int lonEnc, long ord, int docID) throws IOException {
    out.writeInt(latEnc);
    out.writeInt(lonEnc);
    out.writeLong(ord);
    out.writeInt(docID);
    countWritten++;
  }

  @Override
  public LatLonReader getReader(long start) throws IOException {
    assert closed;
    return new OfflineLatLonReader(tempFile, start, count-start);
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
    return "OfflineLatLonWriter(count=" + count + " tempFile=" + tempFile + ")";
  }
}

