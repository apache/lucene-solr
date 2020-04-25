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
package org.apache.solr.common.util;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.solr.SolrTestCase;


class MemWriter extends FastWriter {
  public List<char[]> buffers = new LinkedList<>();

  Random r;
  public MemWriter(char[] tempBuffer, Random r) {
    super(null, tempBuffer, 0);
    this.r = r;
  }

  @Override
  public void flush(char[] arr, int offset, int len) throws IOException {
    if (arr == buf && offset==0 && len==buf.length) {
      buffers.add(buf);  // steal the buffer
      buf = new char[r.nextInt(9000)+1];
    } else if (len > 0) {
      char[] newBuf = new char[len];
      System.arraycopy(arr, offset, newBuf, 0, len);
      buffers.add(newBuf);
    }
  }

  @Override
  public void flush(String str, int offset, int len) throws IOException {
    if (len == 0) return;
    buffers.add( str.substring(offset, offset+len).toCharArray() );
  }
}



public class TestFastWriter extends SolrTestCase {

  Random rand;
  char[] arr;
  String s;

  public void testRandomWrites() throws Exception {
    rand = random();

    arr = new char[20000];
    for (int i=0; i<arr.length; i++) {
      arr[i] = (char)rand.nextInt();
    }
    s = new String(arr);

    for (int i=0; i<1000; i++) {
      doRandomWrites();
    }
  }


  public void doRandomWrites() throws Exception {
    int bufSize = ( rand.nextBoolean() ? rand.nextInt(10) : rand.nextInt(20000) )+1;
    MemWriter out = new MemWriter(new char[bufSize], rand);

    int hash = 0;
    long written = 0;
    int iter = rand.nextInt(20)+1;
    for (int i=0; i<iter; i++) {
      int which = rand.nextInt(3);


      int off = rand.nextInt(arr.length);
      int len = off < arr.length ? rand.nextInt(arr.length - off) : 0;



      if (which == 0) {
        out.write(arr, off, len);
      } else if (which == 1) {
        out.write(s, off, len);
      } else {
        len = 1;
        out.write(arr[off]);
      }

      hash = incHash(hash, arr, off, len);
      written += len;
    }

    out.close();

    int hash2 = 0;
    for (char[] buffer : out.buffers) {
      hash2 = incHash(hash2, buffer, 0, buffer.length);
    }

    assertEquals(hash, hash2);
  }


  public int incHash(int hash, char[] arr, int off, int len) {
    for (int i=off; i<off+len; i++) {
      hash = hash * 31 + arr[i];
    }
    return hash;
  }
}

