package org.apache.solr.util;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import junit.framework.TestCase;

public class ArraysUtilsTest extends TestCase {


  public ArraysUtilsTest(String s) {
    super(s);
  }

  protected void setUp() {
  }

  protected void tearDown() {

  }

  public void test() {
    String left = "this is equal";
    String right = left;
    char[] leftChars = left.toCharArray();
    char[] rightChars = right.toCharArray();
    assertTrue(left + " does not equal: " + right, ArraysUtils.equals(leftChars, 0, rightChars, 0, left.length()));
    
    assertFalse(left + " does not equal: " + right, ArraysUtils.equals(leftChars, 1, rightChars, 0, left.length()));
    assertFalse(left + " does not equal: " + right, ArraysUtils.equals(leftChars, 1, rightChars, 2, left.length()));

    assertFalse(left + " does not equal: " + right, ArraysUtils.equals(leftChars, 25, rightChars, 0, left.length()));
    assertFalse(left + " does not equal: " + right, ArraysUtils.equals(leftChars, 12, rightChars, 0, left.length()));
  }
}