package org.apache.lucene.codecs.pfor;

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

import java.util.Collections;
import java.util.Arrays;
import java.util.Random;
import java.nio.IntBuffer;
import java.nio.ByteBuffer;
import org.apache.lucene.codecs.pfor.PForUtil;
import org.apache.lucene.codecs.pfor.ForPostingsFormat;
import org.apache.lucene.util.LuceneTestCase;

/**
 * Test the core utility for PFor compress and decompress
 * We don't provide test case for For encoder/decoder, since
 * PFor is a extended version of For, and most methods will be reused 
 * here.
 */
public class TestPForUtil extends LuceneTestCase {
  static final int[] MASK={ 0x00000000,
    0x00000001, 0x00000003, 0x00000007, 0x0000000f, 0x0000001f, 0x0000003f,
    0x0000007f, 0x000000ff, 0x000001ff, 0x000003ff, 0x000007ff, 0x00000fff,
    0x00001fff, 0x00003fff, 0x00007fff, 0x0000ffff, 0x0001ffff, 0x0003ffff,
    0x0007ffff, 0x000fffff, 0x001fffff, 0x003fffff, 0x007fffff, 0x00ffffff,
    0x01ffffff, 0x03ffffff, 0x07ffffff, 0x0fffffff, 0x1fffffff, 0x3fffffff,
    0x7fffffff, 0xffffffff};
  Random gen;
  public void initRandom() {
    this.gen = random();
  }

  /**
   * Test correctness of ignored forced exception.
   * The trailing forced exceptions shouldn't be reverted
   * since they're not necessary. 
   */
  public void testForcedException() throws Exception {
    initRandom();
    int sz=ForPostingsFormat.DEFAULT_BLOCK_SIZE;
    Integer[] buff= new Integer[sz];
    int[] data = new int[sz];
    int[] copy = new int[sz];
    byte[] res = new byte[4+sz*8];
    IntBuffer resBuffer = ByteBuffer.wrap(res).asIntBuffer();
    for (int i=0; i<sz-1; ++i)
      buff[i]=gen.nextInt() & 1;
    buff[sz-1]=gen.nextInt() & 0xffffffff;   // create only one exception

    Collections.shuffle(Arrays.asList(buff),gen);
    for (int i=0; i<sz; ++i)
      data[i] = buff[i];

    int ensz = PForUtil.compress(data,sz,resBuffer);

    assert (ensz <= sz*8+4);  // must not exceed the loose upperbound

    resBuffer.rewind();
    PForUtil.decompress(resBuffer,copy);

    //println(getHex(data,sz)+"\n");
    //println(getHex(res,ensz)+"\n");
    //println(getHex(copy,sz)+"\n");
    
    assert cmp(data,sz,copy,sz)==true;
  }

  /**
   * Test correctness of compressing and decompressing.
   * Here we randomly assign a rate of exception (i.e. 1-alpha), 
   * and test different scale of normal/exception values.
   */
  public void testAllDistribution() throws Exception {
    initRandom();
    for (int i=0; i<=32; ++i) { // try to test every kinds of distribution
      double alpha=gen.nextDouble(); // rate of normal value
      for (int j=0; j<=32; ++j) {
        tryDistribution(ForPostingsFormat.DEFAULT_BLOCK_SIZE,alpha,MASK[i],MASK[j]);
      }
    }
  }
  public void tryDistribution(int sz, double alpha, int masknorm, int maskexc) throws Exception {
    Integer[] buff= new Integer[sz];
    int[] data = new int[sz];
    byte[] res = new byte[4+sz*8];      // loosely upperbound
    IntBuffer resBuffer = ByteBuffer.wrap(res).asIntBuffer();
    int i=0;
    for (; i<sz*alpha; ++i)
      buff[i]=gen.nextInt() & masknorm;
    for (; i<sz; ++i)
      buff[i]=gen.nextInt() & maskexc;
    Collections.shuffle(Arrays.asList(buff),gen);
    for (i=0; i<sz; ++i)
      data[i] = buff[i];

    int ensz = PForUtil.compress(data,sz,resBuffer);
    
    assert (ensz <= sz*8+4);  // must not exceed the loose upperbound

    int[] copy = new int[sz];
    PForUtil.decompress(resBuffer,copy);

//    println(getHex(data,sz)+"\n");
//    println(getHex(res,ensz)+"\n");
//    println(getHex(copy,sz)+"\n");

    assert cmp(data,sz,copy,sz)==true;
  }
  public boolean cmp(int[] a, int sza, int[] b, int szb) {
    if (sza!=szb)
      return false;
    for (int i=0; i<sza; ++i) {
      if (a[i]!=b[i]) {
        System.err.println(String.format("! %08x != %08x in %d",a[i],b[i],i));
        return false;
      }
    }
    return true;
  }
  public static String getHex( byte [] raw, int sz ) {
    final String HEXES = "0123456789ABCDEF";
    if ( raw == null ) {
      return null;
    }
    final StringBuilder hex = new StringBuilder( 2 * raw.length );
    for ( int i=0; i<sz; i++ ) {
      if (i>0 && (i)%16 == 0)
        hex.append("\n");
      byte b=raw[i];
      hex.append(HEXES.charAt((b & 0xF0) >> 4))
         .append(HEXES.charAt((b & 0x0F)))
         .append(" ");
    }
    return hex.toString();
  }
  public static String getHex( int [] raw, int sz ) {
    if ( raw == null ) {
      return null;
    }
    final StringBuilder hex = new StringBuilder( 4 * raw.length );
    for ( int i=0; i<sz; i++ ) {
      if (i>0 && i%8 == 0)
        hex.append("\n");
      hex.append(String.format("%08x ",raw[i]));
    }
    return hex.toString();
  }
  static void println(String format, Object... args) {
    System.out.println(String.format(format,args)); 
  }
  static void print(String format, Object... args) {
    System.out.print(String.format(format,args)); 
  }
}
