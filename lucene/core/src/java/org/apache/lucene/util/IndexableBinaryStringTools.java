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
 * <p/>
 * The Strings are constructed using a Base 8000h encoding of the original
 * binary data - each char of an encoded String represents a 15-bit chunk
 * from the byte sequence.  Base 8000h was chosen because it allows for all
 * lower 15 bits of char to be used without restriction; the surrogate range 
 * [U+D8000-U+DFFF] does not represent valid chars, and would require
 * complicated handling to avoid them and allow use of char's high bit.
 * <p/>
 * Although unset bits are used as padding in the final char, the original
 * byte sequence could contain trailing bytes with no set bits (null bytes):
 * padding is indistinguishable from valid information.  To overcome this
 * problem, a char is appended, indicating the number of encoded bytes in the
 * final content char.
 * <p/>
 * Some methods in this class are defined over CharBuffers and ByteBuffers, but
 * these are deprecated in favor of methods that operate directly on byte[] and
 * char[] arrays.  Note that this class calls array() and arrayOffset()
 * on the CharBuffers and ByteBuffers it uses, so only wrapped arrays may be
 * used.  This class interprets the arrayOffset() and limit() values returned 
 * by its input buffers as beginning and end+1 positions on the wrapped array,
 * respectively; similarly, on the output buffer, arrayOffset() is the first
 * position written to, and limit() is set to one past the final output array
 * position.
 * <p/>
 * WARNING: This means that the deprecated Buffer-based methods 
 * only work correctly with buffers that have an offset of 0. For example, they
 * will not correctly interpret buffers returned by {@link ByteBuffer#slice}.  
 *
 * @lucene.experimental
 */
public final class IndexableBinaryStringTools {

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
   * @param original The byte sequence to be encoded. Must be backed by an
   *        array.
   * @return The number of chars required to encode the given byte sequence
   * @throws IllegalArgumentException If the given ByteBuffer is not backed by
   *         an array
   * @deprecated Use {@link #getEncodedLength(byte[], int, int)} instead. This
   *             method will be removed in Lucene 4.0
   */
  @Deprecated
  public static int getEncodedLength(ByteBuffer original)
    throws IllegalArgumentException {
    if (original.hasArray()) {
      return getEncodedLength(original.array(), original.arrayOffset(),
          original.limit() - original.arrayOffset());
    } else {
      throw new IllegalArgumentException("original argument must have a backing array");
    }
  }
  
  /**
   * Returns the number of chars required to encode the given bytes.
   * 
   * @param inputArray byte sequence to be encoded
   * @param inputOffset initial offset into inputArray
   * @param inputLength number of bytes in inputArray
   * @return The number of chars required to encode the number of bytes.
   */
  public static int getEncodedLength(byte[] inputArray, int inputOffset,
      int inputLength) {
    // Use long for intermediaries to protect against overflow
    return (int)((8L * inputLength + 14L) / 15L) + 1;
  }


  /**
   * Returns the number of bytes required to decode the given char sequence.
   * 
   * @param encoded The char sequence to be decoded. Must be backed by an array.
   * @return The number of bytes required to decode the given char sequence
   * @throws IllegalArgumentException If the given CharBuffer is not backed by
   *         an array
   * @deprecated Use {@link #getDecodedLength(char[], int, int)} instead. This
   *             method will be removed in Lucene 4.0
   */
  @Deprecated
  public static int getDecodedLength(CharBuffer encoded) 
    throws IllegalArgumentException {
    if (encoded.hasArray()) {
      return getDecodedLength(encoded.array(), encoded.arrayOffset(), 
          encoded.limit() - encoded.arrayOffset());
    } else {
      throw new IllegalArgumentException("encoded argument must have a backing array");
    }
  }
  
  /**
   * Returns the number of bytes required to decode the given char sequence.
   * 
   * @param encoded char sequence to be decoded
   * @param offset initial offset
   * @param length number of characters
   * @return The number of bytes required to decode the given char sequence
   */
  public static int getDecodedLength(char[] encoded, int offset, int length) {
    final int numChars = length - 1;
    if (numChars <= 0) {
      return 0;
    } else {
      // Use long for intermediaries to protect against overflow
      final long numFullBytesInFinalChar = encoded[offset + length - 1];
      final long numEncodedChars = numChars - 1;
      return (int)((numEncodedChars * 15L + 7L) / 8L + numFullBytesInFinalChar);
    }
  }

  /**
   * Encodes the input byte sequence into the output char sequence. Before
   * calling this method, ensure that the output CharBuffer has sufficient
   * capacity by calling {@link #getEncodedLength(java.nio.ByteBuffer)}.
   * 
   * @param input The byte sequence to encode
   * @param output Where the char sequence encoding result will go. The limit is
   *        set to one past the position of the final char.
   * @throws IllegalArgumentException If either the input or the output buffer
   *         is not backed by an array
   * @deprecated Use {@link #encode(byte[], int, int, char[], int, int)}
   *             instead. This method will be removed in Lucene 4.0
   */
  @Deprecated
  public static void encode(ByteBuffer input, CharBuffer output) {
    if (input.hasArray() && output.hasArray()) {
      final int inputOffset = input.arrayOffset();
      final int inputLength = input.limit() - inputOffset;
      final int outputOffset = output.arrayOffset();
      final int outputLength = getEncodedLength(input.array(), inputOffset,
          inputLength);
      output.limit(outputLength + outputOffset);
      output.position(0);
      encode(input.array(), inputOffset, inputLength, output.array(),
          outputOffset, outputLength);
    } else {
      throw new IllegalArgumentException("Arguments must have backing arrays");
    }
  }
  
  /**
   * Encodes the input byte sequence into the output char sequence.  Before
   * calling this method, ensure that the output array has sufficient
   * capacity by calling {@link #getEncodedLength(byte[], int, int)}.
   * 
   * @param inputArray byte sequence to be encoded
   * @param inputOffset initial offset into inputArray
   * @param inputLength number of bytes in inputArray
   * @param outputArray char sequence to store encoded result
   * @param outputOffset initial offset into outputArray
   * @param outputLength length of output, must be getEncodedLength
   */
  public static void encode(byte[] inputArray, int inputOffset,
      int inputLength, char[] outputArray, int outputOffset, int outputLength) {
    assert (outputLength == getEncodedLength(inputArray, inputOffset,
        inputLength));
    if (inputLength > 0) {
      int inputByteNum = inputOffset;
      int caseNum = 0;
      int outputCharNum = outputOffset;
      CodingCase codingCase;
      for (; inputByteNum + CODING_CASES[caseNum].numBytes <= inputLength; ++outputCharNum) {
        codingCase = CODING_CASES[caseNum];
        if (2 == codingCase.numBytes) {
          outputArray[outputCharNum] = (char) (((inputArray[inputByteNum] & 0xFF) << codingCase.initialShift)
              + (((inputArray[inputByteNum + 1] & 0xFF) >>> codingCase.finalShift) & codingCase.finalMask) & (short) 0x7FFF);
        } else { // numBytes is 3
          outputArray[outputCharNum] = (char) (((inputArray[inputByteNum] & 0xFF) << codingCase.initialShift)
              + ((inputArray[inputByteNum + 1] & 0xFF) << codingCase.middleShift)
              + (((inputArray[inputByteNum + 2] & 0xFF) >>> codingCase.finalShift) & codingCase.finalMask) & (short) 0x7FFF);
        }
        inputByteNum += codingCase.advanceBytes;
        if (++caseNum == CODING_CASES.length) {
          caseNum = 0;
        }
      }
      // Produce final char (if any) and trailing count chars.
      codingCase = CODING_CASES[caseNum];

      if (inputByteNum + 1 < inputLength) { // codingCase.numBytes must be 3
        outputArray[outputCharNum++] = (char) ((((inputArray[inputByteNum] & 0xFF) << codingCase.initialShift) + ((inputArray[inputByteNum + 1] & 0xFF) << codingCase.middleShift)) & (short) 0x7FFF);
        // Add trailing char containing the number of full bytes in final char
        outputArray[outputCharNum++] = (char) 1;
      } else if (inputByteNum < inputLength) {
        outputArray[outputCharNum++] = (char) (((inputArray[inputByteNum] & 0xFF) << codingCase.initialShift) & (short) 0x7FFF);
        // Add trailing char containing the number of full bytes in final char
        outputArray[outputCharNum++] = caseNum == 0 ? (char) 1 : (char) 0;
      } else { // No left over bits - last char is completely filled.
        // Add trailing char containing the number of full bytes in final char
        outputArray[outputCharNum++] = (char) 1;
      }
    }
  }

  /**
   * Decodes the input char sequence into the output byte sequence. Before
   * calling this method, ensure that the output ByteBuffer has sufficient
   * capacity by calling {@link #getDecodedLength(java.nio.CharBuffer)}.
   * 
   * @param input The char sequence to decode
   * @param output Where the byte sequence decoding result will go. The limit is
   *        set to one past the position of the final char.
   * @throws IllegalArgumentException If either the input or the output buffer
   *         is not backed by an array
   * @deprecated Use {@link #decode(char[], int, int, byte[], int, int)}
   *             instead. This method will be removed in Lucene 4.0
   */
  @Deprecated
  public static void decode(CharBuffer input, ByteBuffer output) {
    if (input.hasArray() && output.hasArray()) {
      final int inputOffset = input.arrayOffset();
      final int inputLength = input.limit() - inputOffset;
      final int outputOffset = output.arrayOffset();
      final int outputLength = getDecodedLength(input.array(), inputOffset,
          inputLength);
      output.limit(outputLength + outputOffset);
      output.position(0);
      decode(input.array(), inputOffset, inputLength, output.array(),
          outputOffset, outputLength);
    } else {
      throw new IllegalArgumentException("Arguments must have backing arrays");
    }
  }

  /**
   * Decodes the input char sequence into the output byte sequence. Before
   * calling this method, ensure that the output array has sufficient capacity
   * by calling {@link #getDecodedLength(char[], int, int)}.
   * 
   * @param inputArray char sequence to be decoded
   * @param inputOffset initial offset into inputArray
   * @param inputLength number of chars in inputArray
   * @param outputArray byte sequence to store encoded result
   * @param outputOffset initial offset into outputArray
   * @param outputLength length of output, must be
   *        getDecodedLength(inputArray, inputOffset, inputLength)
   */
  public static void decode(char[] inputArray, int inputOffset,
      int inputLength, byte[] outputArray, int outputOffset, int outputLength) {
    assert (outputLength == getDecodedLength(inputArray, inputOffset,
        inputLength));
    final int numInputChars = inputLength - 1;
    final int numOutputBytes = outputLength;

    if (numOutputBytes > 0) {
      int caseNum = 0;
      int outputByteNum = outputOffset;
      int inputCharNum = inputOffset;
      short inputChar;
      CodingCase codingCase;
      for (; inputCharNum < numInputChars - 1; ++inputCharNum) {
        codingCase = CODING_CASES[caseNum];
        inputChar = (short) inputArray[inputCharNum];
        if (2 == codingCase.numBytes) {
          if (0 == caseNum) {
            outputArray[outputByteNum] = (byte) (inputChar >>> codingCase.initialShift);
          } else {
            outputArray[outputByteNum] += (byte) (inputChar >>> codingCase.initialShift);
          }
          outputArray[outputByteNum + 1] = (byte) ((inputChar & codingCase.finalMask) << codingCase.finalShift);
        } else { // numBytes is 3
          outputArray[outputByteNum] += (byte) (inputChar >>> codingCase.initialShift);
          outputArray[outputByteNum + 1] = (byte) ((inputChar & codingCase.middleMask) >>> codingCase.middleShift);
          outputArray[outputByteNum + 2] = (byte) ((inputChar & codingCase.finalMask) << codingCase.finalShift);
        }
        outputByteNum += codingCase.advanceBytes;
        if (++caseNum == CODING_CASES.length) {
          caseNum = 0;
        }
      }
      // Handle final char
      inputChar = (short) inputArray[inputCharNum];
      codingCase = CODING_CASES[caseNum];
      if (0 == caseNum) {
        outputArray[outputByteNum] = 0;
      }
      outputArray[outputByteNum] += (byte) (inputChar >>> codingCase.initialShift);
      final int bytesLeft = numOutputBytes - outputByteNum;
      if (bytesLeft > 1) {
        if (2 == codingCase.numBytes) {
          outputArray[outputByteNum + 1] = (byte) ((inputChar & codingCase.finalMask) >>> codingCase.finalShift);
        } else { // numBytes is 3
          outputArray[outputByteNum + 1] = (byte) ((inputChar & codingCase.middleMask) >>> codingCase.middleShift);
          if (bytesLeft > 2) {
            outputArray[outputByteNum + 2] = (byte) ((inputChar & codingCase.finalMask) << codingCase.finalShift);
          }
        }
      }
    }
  }

  /**
   * Decodes the given char sequence, which must have been encoded by
   * {@link #encode(java.nio.ByteBuffer)} or
   * {@link #encode(java.nio.ByteBuffer, java.nio.CharBuffer)}.
   * 
   * @param input The char sequence to decode
   * @return A byte sequence containing the decoding result. The limit is set to
   *         one past the position of the final char.
   * @throws IllegalArgumentException If the input buffer is not backed by an
   *         array
   * @deprecated Use {@link #decode(char[], int, int, byte[], int, int)}
   *             instead. This method will be removed in Lucene 4.0
   */
  @Deprecated
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
   * @return A char sequence containing the encoding result. The limit is set to
   *         one past the position of the final char.
   * @throws IllegalArgumentException If the input buffer is not backed by an
   *         array
   * @deprecated Use {@link #encode(byte[], int, int, char[], int, int)}
   *             instead. This method will be removed in Lucene 4.0
   */
  @Deprecated
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
