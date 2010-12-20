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

import java.util.Arrays;

/**
 * Implementation of the optimized PForDelta algorithm for sorted integer arrays. The basic ideas are based on
 * 
 * 1. Original algorithm from
 * http://homepages.cwi.nl/~heman/downloads/msthesis.pdf 
 * 
 * 2. Optimization and
 * variation from http://www2008.org/papers/pdf/p387-zhangA.pdf 
 * 
 * 3. Further optimization
 * http://www2009.org/proceedings/pdf/p401.pdf
 *  
 *  As a part of the PForDelta implementation, Simple16 is used to compress exceptions. The original Simple16 algorithm can also be found in the above literatures.
 * @author hao yan, hyan2008@gmail.com
 */
// nocommit -- must merge our 2 pfor impls before landing on trunk
// nocommit -- need serious random unit test for these int encoders
public class PForDelta{
  
  //All possible values of b in the PForDelta algorithm
  private static final int[] POSSIBLE_B = {0,1,2,3,4,5,6,7,8,9,10,11,12,13,16,20,28}; 
  // Max number of bits to store an uncompressed value
  private static final int MAX_BITS = 32;
  // Header records the value of b and the number of exceptions in the block
  private static final int HEADER_NUM = 1;
  // Header size in bits
  private static final int HEADER_SIZE = MAX_BITS * HEADER_NUM;

  private static final int[] MASK = {0x00000000,
    0x00000001, 0x00000003, 0x00000007, 0x0000000f, 0x0000001f, 0x0000003f,
    0x0000007f, 0x000000ff, 0x000001ff, 0x000003ff, 0x000007ff, 0x00000fff,
    0x00001fff, 0x00003fff, 0x00007fff, 0x0000ffff, 0x0001ffff, 0x0003ffff,
    0x0007ffff, 0x000fffff, 0x001fffff, 0x003fffff, 0x007fffff, 0x00ffffff,
    0x01ffffff, 0x03ffffff, 0x07ffffff, 0x0fffffff, 0x1fffffff, 0x3fffffff,
    0x7fffffff, 0xffffffff};
  
  /**
   * Compress one block of blockSize integers using PForDelta with the optimal parameter b
   * @param inBlock the block to be compressed
   * @param blockSize the block size
   * @return the compressed block
   */
  public static int[] compressOneBlock(final int[] inBlock, int blockSize)
  { 
    // find the best b that can lead to the smallest overall compressed size
    int currentB = POSSIBLE_B[0];   
    int tmpB = currentB;
    int optSize = estimateCompressedSize(inBlock, tmpB, blockSize);
    for (int i = 1; i < POSSIBLE_B.length; ++i)
    {
      tmpB = POSSIBLE_B[i];
      int curSize = estimateCompressedSize(inBlock, tmpB, blockSize);
      if(curSize < optSize)
      {
        currentB = tmpB;
        optSize = curSize;
      }
    }
    
    // compress the block using the above best b 
    int[] outBlock = compressOneBlockCore(inBlock, currentB, blockSize);

    return outBlock;  
  }

  /**
   * Decompress one block using PForDelta
   * @param inBlock the block to be decompressed
   * @param blockSize the number of elements in the decompressed block
   * @return the decompressed block
   */
  public static int[] decompressOneBlock(int[] inBlock, int blockSize)
  {
    int[] expPos = new int[blockSize];
    int[] expHighBits = new int[blockSize];
    int[] outBlock = new int[blockSize];
    assert inBlock != null;
    /*
    if(inBlock == null)
    {
      System.out.println("error: compBlock is null");
      return null;
    }
    */

    int expNum = inBlock[0] & 0x3ff; 
    int bits = (inBlock[0]>>>10) & (0x1f);    

    // decompress the b-bit slots
    int offset = HEADER_SIZE;
    int compressedBits = 0;
    if(bits == 0)
    {
      Arrays.fill(outBlock,0);
    }
    else
    {
      compressedBits = decompressBBitSlots(outBlock, inBlock, blockSize, bits);
    }
    offset += compressedBits;

    // decompress exceptions
    if(expNum>0)
    {
      compressedBits = decompressBlockByS16(expPos, inBlock, offset, expNum);
      offset += compressedBits;
      compressedBits = decompressBlockByS16(expHighBits, inBlock, offset, expNum);
      offset += compressedBits;

      for (int i = 0; i < expNum; i++) 
      { 
        int curExpPos = expPos[i]  ;
        int curHighBits = expHighBits[i];
        outBlock[curExpPos] = (outBlock[curExpPos] & MASK[bits]) | ((curHighBits & MASK[32-bits] ) << bits);
      }
    }
    return outBlock;
  }

  /**
   * Estimate the compressed size in ints of a block
   * @param inputBlock the block to be compressed
   * @param bits the value of the parameter b
   * @param blockSize the block size 
   * @return the compressed size in ints
   * @throws IllegalArgumentException
   */
  private static int estimateCompressedSize(int[] inputBlock, int bits, int blockSize) throws IllegalArgumentException {
    int maxNoExp = (1<<bits)-1;
    // Size of the header and the bits-bit slots
    int outputOffset = HEADER_SIZE + bits * blockSize; 
    int expNum = 0;      

    for (int i = 0; i<blockSize; ++i)
    {      
      if (inputBlock[i] > maxNoExp) 
      {
        expNum++;
      }
    }    
    outputOffset += (expNum<<5);

    return outputOffset;
  }
  
  /**
   * The core implementation of compressing a block with blockSize integers using PForDelta with the given parameter b 
   * @param inputBlock the block to be compressed
   * @param bits the the value of the parameter b
   * @param blockSize the block size
   * @return the compressed block
   * @throws IllegalArgumentException
   */
  private static int[] compressOneBlockCore(int[] inputBlock, int bits, int blockSize) throws IllegalArgumentException {
    int[] expPos = new int[blockSize];
    int[] expHighBits = new int[blockSize];

    int maxCompBitSize =  HEADER_SIZE + blockSize * (MAX_BITS  + MAX_BITS + MAX_BITS) + 32;
    int[] tmpCompressedBlock = new int[(maxCompBitSize>>>5)];

    int outputOffset = HEADER_SIZE;
    int expUpperBound = 1<<bits;
    int expNum = 0;      

    // compress the b-bit slots
    for (int i = 0; i<blockSize; ++i)
    {
      assert inputBlock[i] >= 0: "input value is " + inputBlock[i];
      /*
      if(inputBlock[i] < 0)
      {
        System.out.println("haha<0: [" + i +"]" + inputBlock[i]);
      }
      */
      if (inputBlock[i] < expUpperBound) 
      {
        writeBits(tmpCompressedBlock, inputBlock[i], outputOffset, bits);
      } 
      else // exp
      {
        // store the lower bits-bits of the exception
        writeBits(tmpCompressedBlock, inputBlock[i] & MASK[bits], outputOffset, bits); 
        // write the position of exception
        expPos[expNum] = i; 
        // write the higher 32-bits bits of the exception
        expHighBits[expNum] = (inputBlock[i] >>> bits) & MASK[32-bits];  
        expNum++;
      }
      outputOffset += bits;
    }    

    // the first int in the compressed block stores the value of b and the number of exceptions
    tmpCompressedBlock[0] = ((bits & MASK[10]) << 10) | (expNum & 0x3ff);

    // compress exceptions
    if(expNum>0)
    {
      int compressedBitSize = compressBlockByS16(tmpCompressedBlock, outputOffset, expPos, expNum, blockSize, inputBlock);
      outputOffset += compressedBitSize;
      compressedBitSize = compressBlockByS16(tmpCompressedBlock, outputOffset, expHighBits, expNum, blockSize, inputBlock);
      outputOffset += compressedBitSize;
    }

    // discard the redundant parts in the tmpCompressedBlock
    int compressedSizeInInts = (outputOffset+31)>>>5;
    int[] compBlock;
    compBlock = new int[compressedSizeInInts];
    System.arraycopy(tmpCompressedBlock,0, compBlock, 0, compressedSizeInInts);

    return compBlock;
  }
  
  /**
   * Decompress b-bit slots
   * @param outDecompSlots decompressed block which is the output
   * @param inCompBlock the compressed block which is the input
   * @param blockSize the block size
   * @param bits the value of the parameter b
   * @return the compressed size in bits of the data that has been decompressed
   */
  private static int decompressBBitSlots(int[] outDecompSlots, int[] inCompBlock, int blockSize, int bits)
  {
    int compressedBitSize = 0;
    int offset = HEADER_SIZE;
    for(int i =0; i<blockSize; i++)
    {
      outDecompSlots[i] = readBits(inCompBlock, offset, bits);
      offset += bits;
    }
    compressedBitSize = bits * blockSize;

    return compressedBitSize;    
  } 

  /**
   * Compress a block of blockSize integers using Simple16 algorithm 
   * @param outCompBlock the compressed block which is the output
   * @param outStartOffsetInBits the start offset in bits of the compressed block 
   * @param inBlock the block to be compressed
   * @param blockSize the block size
   * @return the compressed size in bits
   */
  private static int compressBlockByS16(int[] outCompBlock, int outStartOffsetInBits, int[] inBlock, int blockSize, int oriBlockSize, int[] oriInputBlock)
  {
    int outOffset  = (outStartOffsetInBits+31)>>>5; 
    int num, inOffset=0, numLeft;
    for(numLeft=blockSize; numLeft>0; numLeft -= num)
    {
      num = Simple16.s16Compress(outCompBlock, outOffset, inBlock, inOffset, numLeft, blockSize, oriBlockSize, oriInputBlock);
      assert num >= 0;
      /*
      if(num<0)
      {
        System.out.println("oops: s16 get -1 ");
      }
      */
      outOffset++;
      inOffset += num;
    }
    int compressedBitSize = (outOffset<<5)-outStartOffsetInBits;
    return compressedBitSize;    
  }

  /**
   * Decompress a block of blockSize integers using Simple16 algorithm
   * @param outDecompBlock the decompressed block which is the output
   * @param inCompBlock the compressed block which is the input
   * @param blockSize the block size 
   * @param inStartOffsetInBits  the start offset in bits of the compressed block
   * @return the compressed size in bits of the data that has been decompressed
   */
  private static int decompressBlockByS16(int[] outDecompBlock, int[] inCompBlock, int inStartOffsetInBits, int blockSize)
  {    
    int inOffset  = (inStartOffsetInBits+31)>>>5;
    int num, outOffset=0, numLeft;
    for(numLeft=blockSize; numLeft>0; numLeft -= num)
    {
      num = Simple16.s16Decompress(outDecompBlock, outOffset, inCompBlock, inOffset, numLeft);
      outOffset += num;
      inOffset++;
    }
    int compressedBitSize = (inOffset<<5)-inStartOffsetInBits;
    return compressedBitSize;    
  }


  /**
   * Write a certain number of bits of an integer into an integer array starting from the given start offset
   * 
   * @param out the output array 
   * @param val the integer to be written
   * @param outOffset the start offset in bits in the output array
   * @param bits the number of bits to be written (bits>=0)
   */
  private static final void writeBits(int[] out, int val, int outOffset, int bits) {
    if(bits == 0)
      return;
    final int index = outOffset >>> 5;
    final int skip = outOffset & 0x1f;
    val &= (0xffffffff >>> (32 - bits));   
    out[index] |= (val << skip);
    if (32 - skip < bits) {
      out[index + 1] |= (val >>> (32 - skip));
    }
  }

  /**
   * Read a certain number of bits of an integer into an integer array starting from the given start offset
   * 
   * @param in the input array 
   * @param val the integer to be read
   * @param inOffset the start offset in bits in the input array
   * @param bits the number of bits to be read, unlike writeBits(), readBits() does not deal with bits==0 and thus bits must > 0. When bits ==0, the calling functions will just skip the entire bits-bit slots without decoding them
   * @return the bits bits of the input
   */
  private static final int readBits(int[] in, final int inOffset, final int bits) {
    final int index = inOffset >>> 5;
    final int skip = inOffset & 0x1f;
    int val = in[index] >>> skip;
    if (32 - skip < bits) {      
      val |= (in[index + 1] << (32 - skip));
    }
    return val & (0xffffffff >>> (32 - bits));
  }

}


