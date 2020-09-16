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

import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.SuppressForbidden;

/**
 * This class is a workaround for JDK bug
 * <a href="https://bugs.openjdk.java.net/browse/JDK-8252739">JDK-8252739</a>.
 */
@FunctionalInterface
interface BugfixDeflater_JDK8252739 {
  
  public static final boolean IS_BUGGY_JDK = detectBuggyJDK();

  /**
   * Creates a bugfix for {@link Deflater} instances, which works around JDK-8252739.
   * <p>
   * Use this whenever you intend to call {@link Deflater#setDictionary(byte[], int, int)}
   * on a {@code Deflater}.
   * */
  @SuppressForbidden(reason = "Works around bug, so it must call forbidden method")
  public static BugfixDeflater_JDK8252739 createBugfix(Deflater deflater) {
    if (IS_BUGGY_JDK) {
      final BytesRefBuilder dictBytesScratch = new BytesRefBuilder();
      return (dictBytes, off, len) -> {
        if (off > 0) {
          dictBytesScratch.grow(len);
          System.arraycopy(dictBytes, off, dictBytesScratch.bytes(), 0, len);
          deflater.setDictionary(dictBytesScratch.bytes(), 0, len);
        } else {
          deflater.setDictionary(dictBytes, off, len);
        }
      };
    } else {
      return deflater::setDictionary;
    }
  }
  
  /** Call this method as a workaround */
  void setDictionary(byte[] dictBytes, int off, int len);
  
  @SuppressForbidden(reason = "Detector for the bug, so it must call buggy method")
  static boolean detectBuggyJDK() {
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
