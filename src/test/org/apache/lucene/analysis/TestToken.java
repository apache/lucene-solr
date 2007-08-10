package org.apache.lucene.analysis;

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

import java.io.*;
import junit.framework.*;

public class TestToken extends TestCase {

  public TestToken(String name) {
    super(name);
  }

  public void testToString() throws Exception {
    char[] b = {'a', 'l', 'o', 'h', 'a'};
    Token t = new Token("", 0, 5);
    t.setTermBuffer(b, 0, 5);
    assertEquals("(aloha,0,5)", t.toString());

    t.setTermText("hi there");
    assertEquals("(hi there,0,5)", t.toString());
  }

  public void testMixedStringArray() throws Exception {
    Token t = new Token("hello", 0, 5);
    assertEquals(t.termText(), "hello");
    assertEquals(t.termLength(), 5);
    assertEquals(new String(t.termBuffer(), 0, 5), "hello");
    t.setTermText("hello2");
    assertEquals(t.termLength(), 6);
    assertEquals(new String(t.termBuffer(), 0, 6), "hello2");
    t.setTermBuffer("hello3".toCharArray(), 0, 6);
    assertEquals(t.termText(), "hello3");

    // Make sure if we get the buffer and change a character
    // that termText() reflects the change
    char[] buffer = t.termBuffer();
    buffer[1] = 'o';
    assertEquals(t.termText(), "hollo3");
  }
}
