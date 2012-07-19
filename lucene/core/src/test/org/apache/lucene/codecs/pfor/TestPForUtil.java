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

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Random;

import org.apache.lucene.codecs.pfor.ForPostingsFormat;
import org.apache.lucene.codecs.pfor.PForUtil;
import org.apache.lucene.util.LuceneTestCase;

/**
 * Test the core utility for PFor compress and decompress
 * We don't specially provide test case for For encoder/decoder, since
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
   * Should not encode extra information other than single int
   */
  public void testAllEqual() throws Exception {
    initRandom();
    int sz=ForPostingsFormat.DEFAULT_BLOCK_SIZE;
    int[] data=new int[sz];
    byte[] res = new byte[sz*8];
    int[] copy = new int[sz];
    IntBuffer resBuffer = ByteBuffer.wrap(res).asIntBuffer();
    int ensz;
    int header;

    Arrays.fill(data,gen.nextInt());
    header = ForUtil.compress(data,resBuffer); // test For
    ensz = ForUtil.getEncodedSize(header);
    assert ensz == 4;

    ForUtil.decompress(resBuffer,copy,header);
    assert cmp(data,sz,copy,sz)==true;

    Arrays.fill(data,gen.nextInt());
    header = PForUtil.compress(data,resBuffer); // test PFor
    ensz = PForUtil.getEncodedSize(header);
    assert ensz == 4;

    PForUtil.decompress(resBuffer,copy,header);
    assert cmp(data,sz,copy,sz)==true;
  }

  /**
   * Test correctness of forced exception.
   * the forced ones should exactly fit max chain 
   */
  public void testForcedExceptionDistance() throws Exception {
    initRandom();
    int sz=ForPostingsFormat.DEFAULT_BLOCK_SIZE;
    int[] data=new int[sz];
    byte[] res = new byte[sz*8];
    int[] copy = new int[sz];
    IntBuffer resBuffer = ByteBuffer.wrap(res).asIntBuffer();
    int numBits = gen.nextInt(5)+1;

    int i,j;
    int pace, ensz, header;
    int expect, got;

    // fill exception value with same pace, there should
    // be no forced exceptions.
    createDistribution(data, sz, 1, MASK[numBits], MASK[numBits]);
    pace = 1<<numBits;
    for (i=0,j=0; i<sz; i+=pace) {
      int exc = gen.nextInt();
      data[i] = (exc & 0xffff0000) == 0 ? exc | 0xffff0000 : exc;
      j++;
    }
    header = PForUtil.compress(data,resBuffer);
    ensz = PForUtil.getEncodedSize(header);
    expect = j; 
    got = PForUtil.getExcNum(header);
    assert expect == got: expect+" expected but got "+got;

    // there should exactly one forced exception before each
    // exception when i>0
    createDistribution(data, sz, 1, MASK[numBits], MASK[numBits]);
    pace = (1<<numBits)+1;
    for (i=0,j=0; i<sz; i+=pace) {
      int exc = gen.nextInt();
      data[i] = (exc & 0xffff0000) == 0 ? exc | 0xffff0000 : exc;
      j++;
    }
    header = PForUtil.compress(data,resBuffer);
    ensz = PForUtil.getEncodedSize(header);
    expect = 2*(j-1)+1; 
    got = PForUtil.getExcNum(header);
    assert expect == got: expect+" expected but got "+got;


    // two forced exception  
    createDistribution(data, sz, 1, MASK[numBits], MASK[numBits]);
    pace = (1<<numBits)*2+1;
    for (i=0,j=0; i<sz; i+=pace) {
      int exc = gen.nextInt();
      data[i] = (exc & 0xffff0000) == 0 ? exc | 0xffff0000 : exc;
      j++;
    }
    header = PForUtil.compress(data,resBuffer);
    ensz = PForUtil.getEncodedSize(header);
    expect = 3*(j-1)+1; 
    got = PForUtil.getExcNum(header);
    assert expect == got: expect+" expected but got "+got;

  }
  /**
   * Test correctness of ignored forced exception.
   * The trailing forced exceptions should always be reverted
   * since they're not necessary. 
   */
  public void testTrailingForcedException() throws Exception {
    initRandom();
    int sz=ForPostingsFormat.DEFAULT_BLOCK_SIZE;
    assert sz % 32 == 0;
    Integer[] buff= new Integer[sz];
    int[] data = new int[sz];
    int[] copy = new int[sz];
    byte[] res = new byte[sz*8];
    IntBuffer resBuffer = ByteBuffer.wrap(res).asIntBuffer();

    int excIndex = gen.nextInt(sz/2);
    int excValue = gen.nextInt();
    if ((excValue & 0xffff0000) == 0) {
      excValue |= 0xffff0000; // always prepare a 4 bytes exception
    }

    // make value of numFrameBits to be small, 
    // thus easy to get forced exceptions
    for (int i=0; i<sz; ++i) {
      buff[i]=gen.nextInt() & 1;
    }
    // create only one value exception
    buff[excIndex]=excValue;

    for (int i=0; i<sz; ++i)
      data[i] = buff[i];

    int header = PForUtil.compress(data,resBuffer);
    int ensz = PForUtil.getEncodedSize(header);

    assert (ensz <= sz*8): ensz+" > "+sz*8;  // must not exceed the loose upperbound
    assert (ensz >= 4);       // at least we have an exception, right?

    PForUtil.decompress(resBuffer,copy,header);

//    println(getHex(data,sz)+"\n");
//    println(getHex(res,ensz)+"\n");
//    println(getHex(copy,sz)+"\n");

    // fetch the last int, i.e. last exception.
    int lastExc = (res[ensz-4] << 24) | 
         ((0xff & res[ensz-3]) << 16) | 
         ((0xff & res[ensz-2]) << 8 ) | 
          (0xff & res[ensz-1]);

    // trailing forced exceptions are suppressed, 
    // so the last exception should be what we assigned. 
    assert lastExc==excValue;  
    assert cmp(data,sz,copy,sz)==true;
  }

  /**
   * Test correctness of compressing and decompressing.
   * Here we randomly assign a rate of exception (i.e. 1-alpha), 
   * and test different scale of normal/exception values.
   */
  public void testAllDistribution() throws Exception {
    initRandom();
    int sz = ForPostingsFormat.DEFAULT_BLOCK_SIZE;
    int[] data = new int[sz];
    for (int i=0; i<=32; ++i) { // try to test every kinds of distribution
      double alpha=gen.nextDouble(); // rate of normal value
      for (int j=i; j<=32; ++j) {
        createDistribution(data,sz,alpha,MASK[i],MASK[j]);
        tryCompressAndDecompress(data, sz);
      }
    }
  }
  public void createDistribution(int[] data, int sz, double alpha, int masknorm, int maskexc) {
    Integer[] buff= new Integer[sz];
    int i=0;
    for (; i<sz*alpha; ++i)
      buff[i]=gen.nextInt() & masknorm;
    for (; i<sz; ++i)
      buff[i]=gen.nextInt() & maskexc;
    Collections.shuffle(Arrays.asList(buff),gen);
    for (i=0; i<sz; ++i)
      data[i] = buff[i];
  }
  public void tryCompressAndDecompress(final int[] data, int sz) throws Exception {
    byte[] res = new byte[sz*8];      // loosely upperbound
    IntBuffer resBuffer = ByteBuffer.wrap(res).asIntBuffer();

    int header = PForUtil.compress(data,resBuffer);
    int ensz = PForUtil.getEncodedSize(header);
    
    assert (ensz <= sz*8);  // must not exceed the loose upperbound

    int[] copy = new int[sz];
    PForUtil.decompress(resBuffer,copy,header);

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
        System.err.println(String.format(Locale.ENGLISH, "! %08x != %08x in %d",a[i],b[i],i));
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
      hex.append(String.format(Locale.ENGLISH, "%08x ",raw[i]));
    }
    return hex.toString();
  }
  static void eprintln(String format, Object... args) {
    System.err.println(String.format(Locale.ENGLISH, format,args)); 
  }
  static void println(String format, Object... args) {
    System.out.println(String.format(Locale.ENGLISH, format,args)); 
  }
  static void print(String format, Object... args) {
    System.out.print(String.format(Locale.ENGLISH, format,args)); 
  }
}
