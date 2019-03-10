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
package org.apache.solr.util;

import org.apache.solr.SolrTestCase;
import org.apache.solr.update.MemOutputStream;

import java.util.Random;

public class TestFastOutputStream extends SolrTestCase {

  Random rand;
  byte[] arr;

  public void testRandomWrites() throws Exception {
    rand = random();

    arr = new byte[20000];
    for (int i=0; i<arr.length; i++) {
      arr[i] = (byte)rand.nextInt();
    }

    for (int i=0; i<1000; i++) {
      doRandomWrites();
    }

  }

  public void doRandomWrites() throws Exception {
    int bufSize = ( rand.nextBoolean() ? rand.nextInt(10) : rand.nextInt(20000) )+1;
    MemOutputStream out = new MemOutputStream(new byte[bufSize]);

    int hash = 0;
    long written = 0;
    int iter = rand.nextInt(10)+1;
    for (int i=0; i<iter; i++) {
      int off = rand.nextInt(arr.length);
      int len = off < arr.length ? rand.nextInt(arr.length - off) : 0;
      out.write(arr, off, len);
      hash = incHash(hash, arr, off, len);
      written += len;

      int pos = rand.nextInt(arr.length);
      out.write(arr[pos]);
      hash = incHash(hash, arr, pos, 1);
      written += 1;
    }

    out.close();

    int hash2 = 0;
    for (byte[] buffer : out.buffers) {
      hash2 = incHash(hash2, buffer, 0, buffer.length);
    }

    assertEquals(hash, hash2);
    assertEquals(written, out.written());
    assertEquals(written, out.size());
  }


  public int incHash(int hash, byte[] arr, int off, int len) {
    for (int i=off; i<off+len; i++) {
      hash = hash * 31 + arr[i];
    }
    return hash;
  }
}

