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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.util.RamUsageEstimator;

final class OfflinePointReader implements PointReader {
  final IndexInput in;
  long countLeft;
  private final byte[] packedValue;
  private long ord;
  private int docID;
  final int bytesPerDoc;

  OfflinePointReader(Directory tempDir, String tempFileName, int packedBytesLength, long start, long length) throws IOException {
    bytesPerDoc = packedBytesLength + RamUsageEstimator.NUM_BYTES_LONG + RamUsageEstimator.NUM_BYTES_INT;
    in = tempDir.openInput(tempFileName, IOContext.READONCE);
    long seekFP = start * bytesPerDoc;
    in.seek(seekFP);
    this.countLeft = length;
    packedValue = new byte[packedBytesLength];
  }

  @Override
  public boolean next() throws IOException {
    if (countLeft == 0) {
      return false;
    }
    countLeft--;
    in.readBytes(packedValue, 0, packedValue.length);
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

