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

import org.apache.lucene.util.LuceneTestCase;


/**
 *
 *
 **/
public class ArrayUtilTest extends LuceneTestCase {

  public void testParseInt() throws Exception {
    int test;
    try {
      test = ArrayUtil.parseInt("".toCharArray());
      assertTrue(false);
    } catch (NumberFormatException e) {
      //expected
    }
    try {
      test = ArrayUtil.parseInt("foo".toCharArray());
      assertTrue(false);
    } catch (NumberFormatException e) {
      //expected
    }
    try {
      test = ArrayUtil.parseInt(String.valueOf(Long.MAX_VALUE).toCharArray());
      assertTrue(false);
    } catch (NumberFormatException e) {
      //expected
    }
    try {
      test = ArrayUtil.parseInt("0.34".toCharArray());
      assertTrue(false);
    } catch (NumberFormatException e) {
      //expected
    }

    try {
      test = ArrayUtil.parseInt("1".toCharArray());
      assertTrue(test + " does not equal: " + 1, test == 1);
      test = ArrayUtil.parseInt("-10000".toCharArray());
      assertTrue(test + " does not equal: " + -10000, test == -10000);
      test = ArrayUtil.parseInt("1923".toCharArray());
      assertTrue(test + " does not equal: " + 1923, test == 1923);
      test = ArrayUtil.parseInt("-1".toCharArray());
      assertTrue(test + " does not equal: " + -1, test == -1);
      test = ArrayUtil.parseInt("foo 1923 bar".toCharArray(), 4, 4);
      assertTrue(test + " does not equal: " + 1923, test == 1923);
    } catch (NumberFormatException e) {
      e.printStackTrace();
      assertTrue(false);
    }

  }

}
