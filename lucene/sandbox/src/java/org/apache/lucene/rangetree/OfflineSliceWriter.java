package org.apache.lucene.rangetree;

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

import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;

final class OfflineSliceWriter implements SliceWriter {

  final Directory tempDir;
  final byte[] scratchBytes = new byte[RangeTreeWriter.BYTES_PER_DOC];
  final ByteArrayDataOutput scratchBytesOutput = new ByteArrayDataOutput(scratchBytes);      
  final IndexOutput tempFile;
  final long count;
  private boolean closed;
  private long countWritten;

  public OfflineSliceWriter(Directory tempDir, String tempFileNamePrefix, long count) throws IOException {
    this.tempDir = tempDir;
    tempFile = tempDir.createTempOutput(tempFileNamePrefix, "rangetree", IOContext.DEFAULT);
    this.count = count;
  }
    
  @Override
  public void append(long value, long ord, int docID) throws IOException {
    tempFile.writeLong(value);
    tempFile.writeLong(ord);
    tempFile.writeInt(docID);
    countWritten++;
  }

  @Override
  public SliceReader getReader(long start) throws IOException {
    assert closed;
    return new OfflineSliceReader(tempDir, tempFile.getName(), start, count-start);
  }

  @Override
  public void close() throws IOException {
    closed = true;
    tempFile.close();
    if (count != countWritten) {
      throw new IllegalStateException("wrote " + countWritten + " values, but expected " + count);
    }
  }

  @Override
  public void destroy() throws IOException {
    tempDir.deleteFiles(Collections.singleton(tempFile.getName()));
  }

  @Override
  public String toString() {
    return "OfflineSliceWriter(count=" + count + " tempFileName=" + tempFile.getName() + ")";
  }
}

