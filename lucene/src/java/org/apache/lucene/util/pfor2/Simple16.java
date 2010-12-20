package org.apache.lucene.util.pfor2;

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

/**
 * Implementation of the Simple16 algorithm for sorted integer arrays. The basic ideas are based on papers from
 * 
 * 1. http://www2008.org/papers/pdf/p387-zhangA.pdf 
 * 
 * 2. http://www2009.org/proceedings/pdf/p401.pdf
 * 
 */

public class Simple16 {
  
  private static final int S16_NUMSIZE = 16;
  private static final int S16_BITSSIZE = 28;
  // the possible number of bits used to represent one integer 
  private static final int[] S16_NUM = {28, 21, 21, 21, 14, 9, 8, 7, 6, 6, 5, 5, 4, 3, 2, 1};
  // the corresponding number of elements for each value of the number of bits
  private static final int[][] S16_BITS = { {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1},
      {2,2,2,2,2,2,2,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,0,0,0,0,0,0},
      {1,1,1,1,1,1,1,2,2,2,2,2,2,2,1,1,1,1,1,1,1,0,0,0,0,0,0,0},
      {1,1,1,1,1,1,1,1,1,1,1,1,1,1,2,2,2,2,2,2,2,0,0,0,0,0,0,0},
      {2,2,2,2,2,2,2,2,2,2,2,2,2,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0},
      {4,3,3,3,3,3,3,3,3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0},
      {3,4,4,4,4,3,3,3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0},
      {4,4,4,4,4,4,4,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0},
      {5,5,5,5,4,4,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0},
      {4,4,5,5,5,5,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0},
      {6,6,6,5,5,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0},
      {5,5,6,6,6,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0},
      {7,7,7,7,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0},
      {10,9,9,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0},
      {14,14,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0},
      {28,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0} };
  
  /**
   * Compress an integer array using Simple16
   * 
   * @param out the compressed output 
   * @param outOffset the offset of the output in the number of integers
   * @param in the integer input array
   * @param inOffset the offset of the input in the number of integers
   * @param n the number of elements to be compressed
   * @return the number of compressed integers
   */ 
  public static final int s16Compress(int[] out, int outOffset, int[] in, int inOffset, int n, int blockSize, int oriBlockSize, int[] oriInputBlock)
  {
    int numIdx, j, num, bits;
    for (numIdx = 0; numIdx < S16_NUMSIZE; numIdx++) 
    { 
      out[outOffset] = numIdx<<S16_BITSSIZE;
      num = (S16_NUM[numIdx] < n) ? S16_NUM[numIdx] : n; 
      
      for (j = 0, bits = 0; (j < num) && in[inOffset+j] < (1<<S16_BITS[numIdx][j]); ) 
      { 
        out[outOffset] |= (in[inOffset+j]<<bits); 
        bits += S16_BITS[numIdx][j]; 
        j++;
      }
      
      if (j == num) 
      { 
        return num;
      } 
    } 

    return -1;
  }
  
  /**
   * Decompress an integer array using Simple16
   * 
   * @param out the decompressed output 
   * @param outOffset the offset of the output in the number of integers
   * @param in the compressed input array
   * @param inOffset the offset of the input in the number of integers
   * @param n the number of elements to be compressed
   * @return the number of processed integers
   */ 
  public static final int s16Decompress(int[] out, int outOffset, int[] in, int inOffset, int n)
  {
     int numIdx, j=0, bits=0;
     numIdx = in[inOffset]>>>S16_BITSSIZE;
     int num = S16_NUM[numIdx] < n ? S16_NUM[numIdx] : n;
     for(j=0, bits=0; j<num; j++)
     {
       out[outOffset+j]  = readBitsForS16(in, inOffset, bits,  S16_BITS[numIdx][j]);
       bits += S16_BITS[numIdx][j];
     }
     return num;
  }
  

    /**
     * Read a certain number of bits of a integer on the input array
     * @param in the input array
     * @param inIntOffset the start offset in ints in the input array
     * @param inWithIntOffset the start offset within a int in the input array
     * @param bits the number of bits to be read
     * @return the bits bits of the input
     */
   static private int readBitsForS16(int[] in, final int inIntOffset, final int inWithIntOffset, final int bits) {
     final int val = (in[inIntOffset] >>> inWithIntOffset);
     return val & (0xffffffff >>> (32 - bits));
   }
  }
