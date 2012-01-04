package org.apache.lucene.analysis.kuromoji.dict;

/**
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
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.CodecUtil;

public class ConnectionCosts {
  
  public static final String FILENAME = "cc.dat";
  public static final String HEADER = "kuromoji_cc";
  public static final int VERSION = 1;
  
  private short[][] costs; // array is backward IDs first since get is called using the same backward ID consecutively. maybe doesn't matter.
  
  public ConnectionCosts() {
  }
  
  private ConnectionCosts(short[][] costs) {
    this.costs = costs;
  }
  
  public ConnectionCosts(int forwardSize, int backwardSize) {
    this.costs = new short[backwardSize][forwardSize]; 
  }
  
  public void add(int forwardId, int backwardId, int cost) {
    this.costs[backwardId][forwardId] = (short)cost;
  }
  
  public int get(int forwardId, int backwardId) {
    // FIXME: There seems to be something wrong with the double array trie in some rare
    // cases causing and IndexOutOfBoundsException.  Use a guard as a temporary work-around
    // and return a high cost to advise Mr. Viterbi strongly to not use this transition
    if (backwardId < costs.length && forwardId < costs[backwardId].length ) {
      return costs[backwardId][forwardId];
    } else {
      return 50000;
    }
  }
  
  public void write(String directoryname) throws IOException {
    String filename = directoryname + File.separator + FILENAME;
    OutputStream os = new FileOutputStream(filename);
    try {
      os = new BufferedOutputStream(os);
      final DataOutput out = new OutputStreamDataOutput(os);
      CodecUtil.writeHeader(out, HEADER, VERSION);
      out.writeVInt(costs.length);
      for (short[] a : costs) {
        out.writeVInt(a.length);
        for (int i = 0; i < a.length; i++) {
          out.writeShort(a[i]);
        }
      }
    } finally {
      os.close();
    }
  }
  
  public static ConnectionCosts getInstance() throws IOException, ClassNotFoundException {
    InputStream is = ConnectionCosts.class.getResourceAsStream(FILENAME);
    return read(is);
  }
  
  public static ConnectionCosts read(InputStream is) throws IOException, ClassNotFoundException {
    is = new BufferedInputStream(is);
    try {
      final DataInput in = new InputStreamDataInput(is);
      CodecUtil.checkHeader(in, HEADER, VERSION, VERSION);
      final short[][] costs = new short[in.readVInt()][];
      for (int j = 0; j < costs.length; j++) {
        final int len = in.readVInt();
        final short[] a = new short[len];
        for (int i = 0; i < len; i++) {
          a[i] = in.readShort();
        }
        costs[j] = a;
      }
      return new ConnectionCosts(costs);
    } finally {
      is.close();
    }
  }
  
}
