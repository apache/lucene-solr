package org.apache.lucene.util.encoding;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

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

/**
 * Tests the {@link VInt8} class.
 */
public class Vint8Test extends LuceneTestCase {

  private static final int[] TEST_VALUES = {
    -1000000000,
    -1, 0, (1 << 7) - 1, 1 << 7, (1 << 14) - 1, 1 << 14,
    (1 << 21) - 1, 1 << 21, (1 << 28) - 1, 1 << 28
  };
  private static int[] BYTES_NEEDED_TEST_VALUES = {
    5, 5, 1, 1, 2, 2, 3, 3, 4, 4, 5
  };

  @Test
  public void testBytesRef() throws Exception {
    BytesRef bytes = new BytesRef(256);
    int expectedSize = 0;
    for (int j = 0; j < TEST_VALUES.length; j++) {
      VInt8.encode(TEST_VALUES[j], bytes);
      expectedSize += BYTES_NEEDED_TEST_VALUES[j];
    }
    assertEquals(expectedSize, bytes.length);
    
    for (int j = 0; j < TEST_VALUES.length; j++) {
      assertEquals(TEST_VALUES[j], VInt8.decode(bytes));
    }
    assertEquals(bytes.offset, bytes.length);
  }
  
}
