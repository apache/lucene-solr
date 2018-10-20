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
package org.apache.lucene.analysis;

import java.io.IOException;
import java.io.Reader;
import java.util.Random;

import org.apache.lucene.util.TestUtil;

/** Wraps a Reader, and can throw random or fixed
 *  exceptions, and spoon feed read chars. */

public class MockReaderWrapper extends Reader {
  
  private final Reader in;
  private final Random random;

  private int excAtChar = -1;
  private int readSoFar;
  private boolean throwExcNext;

  public MockReaderWrapper(Random random, Reader in) {
    this.in = in;
    this.random = random;
  }

  /** Throw an exception after reading this many chars. */
  public void throwExcAfterChar(int charUpto) {
    excAtChar = charUpto;
    // You should only call this on init!:
    assert readSoFar == 0;
  }

  public void throwExcNext() {
    throwExcNext = true;
  }

  @Override
  public void close() throws IOException {
    in.close();
  }

  @Override
  public int read(char[] cbuf, int off, int len) throws IOException {
    if (throwExcNext || (excAtChar != -1 && readSoFar >= excAtChar)) {
      throw new RuntimeException("fake exception now!");
    }
    final int read;
    final int realLen;
    if (len == 1) {
      realLen = 1;
    } else {
      // Spoon-feed: intentionally maybe return less than
      // the consumer asked for
      realLen = TestUtil.nextInt(random, 1, len);
    }
    if (excAtChar != -1) {
      final int left = excAtChar - readSoFar;
      assert left != 0;
      read = in.read(cbuf, off, Math.min(realLen, left));
      assert read != -1;
      readSoFar += read;
    } else {
      read = in.read(cbuf, off, realLen);
    }
    return read;
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  @Override
  public boolean ready() {
    return false;
  }

  public static boolean isMyEvilException(Throwable t) {
    return (t instanceof RuntimeException) && "fake exception now!".equals(t.getMessage());
  }
};
