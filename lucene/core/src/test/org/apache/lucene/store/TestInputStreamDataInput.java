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
package org.apache.lucene.store;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class TestInputStreamDataInput extends LuceneTestCase {
  private static byte[] RANDOM_DATA;
  private InputStreamDataInput in;

  @BeforeClass
  public static void beforeClass() {
    RANDOM_DATA = new byte[atLeast(100)];
    random().nextBytes(RANDOM_DATA);
  }

  @AfterClass
  public static void afterClass() {
    RANDOM_DATA = null;
  }

  @Before
  public void before() {
    in = new NoReadInputStreamDataInput(new ByteArrayInputStream(RANDOM_DATA));
  }

  @After
  public void after() throws IOException {
    in.close();
    in = null;
  }

  public void testSkipBytes() throws IOException {
    Random random = random();
    // not using the wrapped (NoReadInputStreamDataInput) here since we want to actually read and
    // verify
    InputStreamDataInput in = new InputStreamDataInput(new ByteArrayInputStream(RANDOM_DATA));
    int maxSkipTo = RANDOM_DATA.length - 1;
    // skip chunks of bytes until exhausted
    for (int curr = 0; curr < maxSkipTo; ) {
      int skipTo = TestUtil.nextInt(random, curr, maxSkipTo);
      int step = skipTo - curr;
      in.skipBytes(step);
      assertEquals(RANDOM_DATA[skipTo], in.readByte());
      curr = skipTo + 1; // +1 for read byte
    }
    in.close();
  }

  public void testNoReadWhenSkipping() throws IOException {
    Random random = random();
    int maxSkipTo = RANDOM_DATA.length - 1;
    // skip chunks of bytes until exhausted
    for (int curr = 0; curr < maxSkipTo; ) {
      int step = TestUtil.nextInt(random, 0, maxSkipTo - curr);
      in.skipBytes(step);
      curr += step;
    }
  }

  public void testFullSkip() throws IOException {
    in.skipBytes(RANDOM_DATA.length);
  }

  public void testSkipOffEnd() {
    expectThrows(EOFException.class, () -> in.skipBytes(RANDOM_DATA.length + 1));
  }

  /** Throws if trying to read bytes to ensure skipBytes doesn't invoke read */
  private static final class NoReadInputStreamDataInput extends InputStreamDataInput {

    public NoReadInputStreamDataInput(InputStream is) {
      super(is);
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte readByte() {
      throw new UnsupportedOperationException();
    }
  }
}
