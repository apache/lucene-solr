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
package org.apache.lucene.facet.taxonomy.writercache;

import org.apache.lucene.util.LuceneTestCase.Monster;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

@Monster("uses lots of space and takes a few minutes")
public class Test2GBCharBlockArray extends LuceneTestCase {

  public void test2GBChars() throws Exception {
    int blockSize = 32768;
    CharBlockArray array = new CharBlockArray(blockSize);

    int size = TestUtil.nextInt(random(), 20000, 40000);

    char[] chars = new char[size];
    int count = 0;
    while (true) {
      count++;
      try {
        array.append(chars, 0, size);
      } catch (IllegalStateException ise) {
        assertTrue(count * (long) size + blockSize > Integer.MAX_VALUE);
        break;
      }
      assertFalse("appended " + (count * (long) size - Integer.MAX_VALUE) + " characters beyond Integer.MAX_VALUE!",
                  count * (long) size > Integer.MAX_VALUE);
    }
  }
}
