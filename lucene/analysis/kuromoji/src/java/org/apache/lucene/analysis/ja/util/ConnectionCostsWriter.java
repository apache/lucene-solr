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
package org.apache.lucene.analysis.ja.util;


import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.lucene.analysis.ja.dict.ConnectionCosts;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.OutputStreamDataOutput;

final class ConnectionCostsWriter {
  
  private final short[][] costs; // array is backward IDs first since get is called using the same backward ID consecutively. maybe doesn't matter.
  private final int forwardSize;
  private final int backwardSize;
  /**
   * Constructor for building. TODO: remove write access
   */
  ConnectionCostsWriter(int forwardSize, int backwardSize) {
    this.forwardSize = forwardSize;
    this.backwardSize = backwardSize;
    this.costs = new short[backwardSize][forwardSize];
  }
  
  public void add(int forwardId, int backwardId, int cost) {
    this.costs[backwardId][forwardId] = (short)cost;
  }
  
  public void write(Path baseDir) throws IOException {
    Files.createDirectories(baseDir);
    String fileName = ConnectionCosts.class.getName().replace('.', '/') + ConnectionCosts.FILENAME_SUFFIX;
    try (OutputStream os = Files.newOutputStream(baseDir.resolve(fileName));
         OutputStream bos = new BufferedOutputStream(os)) {
      final DataOutput out = new OutputStreamDataOutput(bos);
      CodecUtil.writeHeader(out, ConnectionCosts.HEADER, ConnectionCosts.VERSION);
      out.writeVInt(forwardSize);
      out.writeVInt(backwardSize);
      int last = 0;
      assert costs.length == backwardSize;
      for (short[] a : costs) {
        assert a.length == forwardSize;
        for (short cost : a) {
          int delta = (int) cost - last;
          out.writeZInt(delta);
          last = cost;
        }
      }
    }
  }
  
}
