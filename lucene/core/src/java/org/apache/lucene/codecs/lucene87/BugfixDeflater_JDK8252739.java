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
package org.apache.lucene.codecs.lucene87;

import java.util.Arrays;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * This class is a workaround for JDK bug
 * <a href="https://bugs.openjdk.java.net/browse/JDK-8252739">JDK-8252739</a>.
 */
final class BugfixDeflater_JDK8252739 extends Deflater {
  
  public static final boolean IS_BUGGY_JDK = detectBuggyJDK();

  /**
   * Creates a {@link Deflater} instance, which works around JDK-8252739.
   * <p>
   * Use this whenever you intend to call {@link #setDictionary(byte[], int, int)} or
   * {@link #setDictionary(java.nio.ByteBuffer)} on a {@code Deflater}.
   * */
  public static Deflater createDeflaterInstance(int level, boolean nowrap, int dictLength) {
    if (dictLength < 0) {
      throw new IllegalArgumentException("dictLength must be >= 0");
    }
    if (IS_BUGGY_JDK) {
      return new BugfixDeflater_JDK8252739(level, nowrap, dictLength);
    } else {
      return new Deflater(level, nowrap);
    }
  }
  
  
  private final byte[] dictBytesScratch;

  private BugfixDeflater_JDK8252739(int level, boolean nowrap, int dictLength) {
    super(level, nowrap);
    this.dictBytesScratch = new byte[dictLength];
  }
  
  @Override
  public void setDictionary(byte[] dictBytes, int off, int len) {
    if (off > 0) {
      System.arraycopy(dictBytes, off, dictBytesScratch, 0, len);
      super.setDictionary(dictBytesScratch, 0, len);
    } else {
      super.setDictionary(dictBytes, off, len);
    }
  }

  private static boolean detectBuggyJDK() {
    final byte[] testData = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };
    final byte[] compressed = new byte[32]; // way enough space
    final Deflater deflater = new Deflater(6, true);
    int compressedSize;
    try {
      deflater.reset();
      deflater.setDictionary(testData, 4, 4);
      deflater.setInput(testData);
      deflater.finish();
      compressedSize = deflater.deflate(compressed, 0, compressed.length, Deflater.FULL_FLUSH);
    } finally {
      deflater.end();
    }
    
    // in nowrap mode we need extra 0-byte as padding, add explicit:
    compressed[compressedSize] = 0;
    compressedSize++;
    
    final Inflater inflater = new Inflater(true);
    final byte[] restored = new byte[testData.length];
    try {
      inflater.reset();
      inflater.setDictionary(testData, 4, 4);
      inflater.setInput(compressed, 0, compressedSize);
      final int restoredLength = inflater.inflate(restored);
      if (restoredLength != testData.length) {
        return true;
      }
    } catch (DataFormatException e) {
      return true;
    } catch(RuntimeException e) {
      return true;
    } finally {
      inflater.end();
    }

    if (Arrays.equals(testData, restored) == false) {
      return true;
    }
    
    // all fine
    return false;
  }
  
}
