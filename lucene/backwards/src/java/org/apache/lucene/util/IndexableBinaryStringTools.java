package org.apache.lucene.util;

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

import java.nio.CharBuffer;
import java.nio.ByteBuffer;

/**
 * Provides support for converting byte sequences to Strings and back again.
 * The resulting Strings preserve the original byte sequences' sort order.
 * 
 * The Strings are constructed using a Base 8000h encoding of the original
 * binary data - each char of an encoded String represents a 15-bit chunk
 * from the byte sequence.  Base 8000h was chosen because it allows for all
 * lower 15 bits of char to be used without restriction; the surrogate range 
 * [U+D8000-U+DFFF] does not represent valid chars, and would require
 * complicated handling to avoid them and allow use of char's high bit.
 * 
 * Although unset bits are used as padding in the final char, the original
 * byte sequence could contain trailing bytes with no set bits (null bytes):
 * padding is indistinguishable from valid information.  To overcome this
 * problem, a char is appended, indicating the number of encoded bytes in the
 * final content char.
 * 
 * This class's operations are defined over CharBuffers and ByteBuffers, to
 * allow for wrapped arrays to be reused, reducing memory allocation costs for
 * repeated operations.  Note that this class calls array() and arrayOffset()
 * on the CharBuffers and ByteBuffers it uses, so only wrapped arrays may be
 * used.  This class interprets the arrayOffset() and limit() values returned by
 * its input buffers as beginning and end+1 positions on the wrapped array,
 * respectively; similarly, on the output buffer, arrayOffset() is the first
 * position written to, and limit() is set to one past the final output array
 * position.
 */
public class IndexableBinaryStringTools {

  private static final CodingCase[] CODING_CASES = {
    // CodingCase(int initialShift, int finalShift)
    new CodingCase( 7, 1   ),
    // CodingCase(int initialShift, int middleShift, int finalShift)
    new CodingCase(14, 6, 2),
    new CodingCase(13, 5, 3),
    new CodingCase(12, 4, 4),
    new CodingCase(11, 3, 5),
    new CodingCase(10, 2, 6),
    new CodingCase( 9, 1, 7),
    new CodingCase( 8, 0   )
  };

  // Export only static methods
  private IndexableBinaryStringTools() {}

  /**
   * Returns the number of chars required to encode the given byte sequence.
   * 
   * @param original The byte sequence to be encoded.  Must be backed by an array.
   * @return The number of chars required to encode the given byte sequence
   * @throws IllegalArgumentException If the given ByteBuffer is not backed by an array
   */
  public static int getEncodedLength(ByteBuffer original) 
    throws IllegalArgumentException {
    if (original.hasArray()) {
      // Use long for intermediaries to protect against overflow
      long length = (long)(original.limit() - original.arrayOffset());
      return (int)((length * 8L + 14L) / 15L) + 1;
    } else {
      throw new IllegalArgumentException("original argument must have a backing array");
    }
  }

  /**
   * Returns the number of bytes required to decode the given char sequence.
   * 
   * @param encoded The char sequence to be encoded.  Must be backed by an array.
   * @return The number of bytes required to decode the given char sequence
   * @throws IllegalArgumentException If the given CharBuffer is not backed by an array
   */
  public static int getDecodedLength(CharBuffer encoded) 
    throws IllegalArgumentException {
    if (encoded.hasArray()) {
      int numChars = encoded.limit() - encoded.arrayOffset() - 1;
      if (numChars <= 0) {
        return 0;
      } else {
        int numFullBytesInFinalChar = encoded.charAt(encoded.limit() - 1);
        int numEncodedChars = numChars - 1;
        return (numEncodedChars * 15 + 7) / 8 + numFullBytesInFinalChar;
      }
    } else {
      throw new IllegalArgumentException("encoded argument must have a backing array");
    }
  }

  /**
   * Encodes the input byte sequence into the output char sequence.  Before
   * calling this method, ensure that the output CharBuffer has sufficient
   * capacity by calling {@link #getEncodedLength(java.nio.ByteBuffer)}.
   * 
   * @param input The byte sequence to encode
   * @param output Where the char sequence encoding result will go.  The limit
   *  is set to one past the position of the final char.
   * @throws IllegalArgumentException If either the input or the output buffer
   *  is not backed by an array
   */
  public static void encode(ByteBuffer input, CharBuffer output) {
    if (input.hasArray() && output.hasArray()) {
      byte[] inputArray = input.array();
      int inputOffset = input.arrayOffset();
      int inputLength = input.limit() - inputOffset; 
      char[] outputArray = output.array();
      int outputOffset = output.arrayOffset();
      int outputLength = getEncodedLength(input);
      output.limit(outputOffset + outputLength); // Set output final pos + 1
      output.position(0);
      if (inputLength > 0) {
        int inputByteNum = inputOffset;
        int caseNum = 0;
        int outputCharNum = outputOffset;
        CodingCase codingCase;
        for ( ; inputByteNum + CODING_CASES[caseNum].numBytes <= inputLength ;
              ++outputCharNum                                                 ) {
          codingCase = CODING_CASES[caseNum];
          if (2 == codingCase.numBytes) {
            outputArray[outputCharNum]
              = (char)(((inputArray[inputByteNum] & 0xFF) << codingCase.initialShift)
                       + (((inputArray[inputByteNum + 1] & 0xFF) >>> codingCase.finalShift)
                          & codingCase.finalMask)
                       & (short)0x7FFF);
          } else { // numBytes is 3
            outputArray[outputCharNum] 
              = (char)(((inputArray[inputByteNum] & 0xFF) << codingCase.initialShift)
                       + ((inputArray[inputByteNum + 1] & 0xFF) << codingCase.middleShift)
                       + (((inputArray[inputByteNum + 2] & 0xFF) >>> codingCase.finalShift) 
                          & codingCase.finalMask)
                       & (short)0x7FFF);          
          }
          inputByteNum += codingCase.advanceBytes;          
          if (++caseNum == CODING_CASES.length) {
            caseNum = 0;
          }
        }
        // Produce final char (if any) and trailing count chars.
        codingCase = CODING_CASES[caseNum];
        
        if (inputByteNum + 1 < inputLength) { // codingCase.numBytes must be 3
          outputArray[outputCharNum++] 
            = (char)((((inputArray[inputByteNum] & 0xFF) << codingCase.initialShift)
                      + ((inputArray[inputByteNum + 1] & 0xFF) << codingCase.middleShift))
                     & (short)0x7FFF);
          // Add trailing char containing the number of full bytes in final char
          outputArray[outputCharNum++] = (char)1;
        } else if (inputByteNum < inputLength) {
          outputArray[outputCharNum++] 
            = (char)(((inputArray[inputByteNum] & 0xFF) << codingCase.initialShift)
                     & (short)0x7FFF);
          // Add trailing char containing the number of full bytes in final char
          outputArray[outputCharNum++] = caseNum == 0 ? (char)1 : (char)0;
        } else { // No left over bits - last char is completely filled.
          // Add trailing char containing the number of full bytes in final char
          outputArray[outputCharNum++] = (char)1;
        }
      }
    } else {
      throw new IllegalArgumentException("Arguments must have backing arrays");
    }
  }

  /**
   * Decodes the input char sequence into the output byte sequence.  Before
   * calling this method, ensure that the output ByteBuffer has sufficient
   * capacity by calling {@link #getDecodedLength(java.nio.CharBuffer)}.
   * 
   * @param input The char sequence to decode
   * @param output Where the byte sequence decoding result will go.  The limit
   *  is set to one past the position of the final char.
   * @throws IllegalArgumentException If either the input or the output buffer
   *  is not backed by an array
   */
  public static void decode(CharBuffer input, ByteBuffer output) {
    if (input.hasArray() && output.hasArray()) {
      int numInputChars = input.limit() - input.arrayOffset() - 1;
      int numOutputBytes = getDecodedLength(input);
      output.limit(numOutputBytes + output.arrayOffset()); // Set output final pos + 1
      output.position(0);
      byte[] outputArray = output.array();
      char[] inputArray = input.array();
      if (numOutputBytes > 0) {
        int caseNum = 0;
        int outputByteNum = output.arrayOffset();
        int inputCharNum = input.arrayOffset();
        short inputChar;
        CodingCase codingCase;
        for ( ; inputCharNum < numInputChars - 1 ; ++inputCharNum) {
          codingCase = CODING_CASES[caseNum];
          inputChar = (short)inputArray[inputCharNum];
          if (2 == codingCase.numBytes) {
            if (0 == caseNum) {
              outputArray[outputByteNum] = (byte)(inputChar >>> codingCase.initialShift);
            } else {
              outputArray[outputByteNum] += (byte)(inputChar >>> codingCase.initialShift);
            }
            outputArray[outputByteNum + 1] = (byte)((inputChar & codingCase.finalMask) 
                                                    << codingCase.finalShift);
          } else { // numBytes is 3
            outputArray[outputByteNum] += (byte)(inputChar >>> codingCase.initialShift);
            outputArray[outputByteNum + 1] = (byte)((inputChar & codingCase.middleMask)
                                                    >>> codingCase.middleShift);
            outputArray[outputByteNum + 2] = (byte)((inputChar & codingCase.finalMask) 
                                                    << codingCase.finalShift);
          }
          outputByteNum += codingCase.advanceBytes;
          if (++caseNum == CODING_CASES.length) {
            caseNum = 0;
          }
        }
        // Handle final char
        inputChar = (short)inputArray[inputCharNum];
        codingCase = CODING_CASES[caseNum];
        if (0 == caseNum) {
          outputArray[outputByteNum] = 0;
        }
        outputArray[outputByteNum] += (byte)(inputChar >>> codingCase.initialShift);
        int bytesLeft = numOutputBytes - outputByteNum;
        if (bytesLeft > 1) {
          if (2 == codingCase.numBytes) {
            outputArray[outputByteNum + 1] = (byte)((inputChar & codingCase.finalMask) 
                                                    >>> codingCase.finalShift);
          } else { // numBytes is 3
            outputArray[outputByteNum + 1] = (byte)((inputChar & codingCase.middleMask)
                                                    >>> codingCase.middleShift);
            if (bytesLeft > 2) {
              outputArray[outputByteNum + 2] = (byte)((inputChar & codingCase.finalMask) 
                                                      << codingCase.finalShift);
            }
          }
        }
      }
    } else {
      throw new IllegalArgumentException("Arguments must have backing arrays");
    }
  }

  /**
   * Decodes the given char sequence, which must have been encoded by
   * {@link #encode(java.nio.ByteBuffer)} or 
   * {@link #encode(java.nio.ByteBuffer, java.nio.CharBuffer)}.
   * 
   * @param input The char sequence to decode
   * @return A byte sequence containing the decoding result.  The limit
   *  is set to one past the position of the final char.
   * @throws IllegalArgumentException If the input buffer is not backed by an
   *  array
   */
  public static ByteBuffer decode(CharBuffer input) {
    byte[] outputArray = new byte[getDecodedLength(input)];
    ByteBuffer output = ByteBuffer.wrap(outputArray);
    decode(input, output);
    return output;
  }

  /**
   * Encodes the input byte sequence.
   * 
   * @param input The byte sequence to encode
   * @return A char sequence containing the encoding result.  The limit is set
   *  to one past the position of the final char.
   * @throws IllegalArgumentException If the input buffer is not backed by an
   *  array
   */
  public static CharBuffer encode(ByteBuffer input) {
    char[] outputArray = new char[getEncodedLength(input)];
    CharBuffer output = CharBuffer.wrap(outputArray);
    encode(input, output);
    return output;
  }
  
  static class CodingCase {
    int numBytes, initialShift, middleShift, finalShift, advanceBytes = 2;
    short middleMask, finalMask;

    CodingCase(int initialShift, int middleShift, int finalShift) {
      this.numBytes = 3;
      this.initialShift = initialShift;
      this.middleShift = middleShift;
      this.finalShift = finalShift;
      this.finalMask = (short)((short)0xFF >>> finalShift);
      this.middleMask = (short)((short)0xFF << middleShift);
    }

    CodingCase(int initialShift, int finalShift) {
      this.numBytes = 2;
      this.initialShift = initialShift;
      this.finalShift = finalShift;
      this.finalMask = (short)((short)0xFF >>> finalShift);
      if (finalShift != 0) {
        advanceBytes = 1; 
      }
    }
  }
}
