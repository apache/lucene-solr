package org.apache.lucene.store;

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

import java.io.IOException;
import java.util.HashMap;

import org.apache.lucene.util.LuceneTestCase;

/** Test huge RAMFile with more than Integer.MAX_VALUE bytes. */
public class TestHugeRamFile extends LuceneTestCase {
  
  private static final long MAX_VALUE = (long) 2 * (long) Integer.MAX_VALUE;

  /** Fake a huge ram file by using the same byte buffer for all 
   * buffers under maxint. */
  private static class DenseRAMFile extends RAMFile {
    private long capacity = 0;
    private HashMap<Integer,byte[]> singleBuffers = new HashMap<Integer,byte[]>();
    @Override
    protected byte[] newBuffer(int size) {
      capacity += size;
      if (capacity <= MAX_VALUE) {
        // below maxint we reuse buffers
        byte buf[] = singleBuffers.get(Integer.valueOf(size));
        if (buf==null) {
          buf = new byte[size]; 
          //System.out.println("allocate: "+size);
          singleBuffers.put(Integer.valueOf(size),buf);
        }
        return buf;
      }
      //System.out.println("allocate: "+size); System.out.flush();
      return new byte[size];
    }
  }
  
  /** Test huge RAMFile with more than Integer.MAX_VALUE bytes. (LUCENE-957) */
  public void testHugeFile() throws IOException {
    DenseRAMFile f = new DenseRAMFile();
    // output part
    RAMOutputStream out = new RAMOutputStream(f);
    byte b1[] = new byte[RAMOutputStream.BUFFER_SIZE];
    byte b2[] = new byte[RAMOutputStream.BUFFER_SIZE / 3];
    for (int i = 0; i < b1.length; i++) {
      b1[i] = (byte) (i & 0x0007F);
    }
    for (int i = 0; i < b2.length; i++) {
      b2[i] = (byte) (i & 0x0003F);
    }
    long n = 0;
    assertEquals("output length must match",n,out.length());
    while (n <= MAX_VALUE - b1.length) {
      out.writeBytes(b1,0,b1.length);
      out.flush();
      n += b1.length;
      assertEquals("output length must match",n,out.length());
    }
    //System.out.println("after writing b1's, length = "+out.length()+" (MAX_VALUE="+MAX_VALUE+")");
    int m = b2.length;
    long L = 12;
    for (int j=0; j<L; j++) {
      for (int i = 0; i < b2.length; i++) {
        b2[i]++;
      }
      out.writeBytes(b2,0,m);
      out.flush();
      n += m;
      assertEquals("output length must match",n,out.length());
    }
    out.close();
    // input part
    RAMInputStream in = new RAMInputStream("testcase", f);
    assertEquals("input length must match",n,in.length());
    //System.out.println("input length = "+in.length()+" % 1024 = "+in.length()%1024);
    for (int j=0; j<L; j++) {
      long loc = n - (L-j)*m; 
      in.seek(loc/3);
      in.seek(loc);
      for (int i=0; i<m; i++) {
        byte bt = in.readByte();
        byte expected = (byte) (1 + j + (i & 0x0003F));
        assertEquals("must read same value that was written! j="+j+" i="+i,expected,bt);
      }
    }
  }
}
