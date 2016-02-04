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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.lucene.store.InputStreamDataInput;

final class OfflineLatLonReader implements LatLonReader {
  final InputStreamDataInput in;
  long countLeft;
  private int latEnc;
  private int lonEnc;
  private long ord;
  private int docID;

  OfflineLatLonReader(Path tempFile, long start, long count) throws IOException {
    InputStream fis = Files.newInputStream(tempFile);
    long seekFP = start * BKDTreeWriter.BYTES_PER_DOC;
    long skipped = 0;
    while (skipped < seekFP) {
      long inc = fis.skip(seekFP - skipped);
      skipped += inc;
      if (inc == 0) {
        throw new RuntimeException("skip returned 0");
      }
    }
    in = new InputStreamDataInput(new BufferedInputStream(fis));
    this.countLeft = count;
  }

  @Override
  public boolean next() throws IOException {
    if (countLeft == 0) {
      return false;
    }
    countLeft--;
    latEnc = in.readInt();
    lonEnc = in.readInt();
    ord = in.readLong();
    docID = in.readInt();
    return true;
  }

  @Override
  public int latEnc() {
    return latEnc;
  }

  @Override
  public int lonEnc() {
    return lonEnc;
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

