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

import java.nio.CharBuffer;

import org.apache.lucene.util.LuceneTestCase;

public class TestReusableStringReader extends LuceneTestCase {
  
  public void test() throws Exception {
    ReusableStringReader reader = new ReusableStringReader();
    assertEquals(-1, reader.read());
    assertEquals(-1, reader.read(new char[1]));
    assertEquals(-1, reader.read(new char[2], 1, 1));
    assertEquals(-1, reader.read(CharBuffer.wrap(new char[2])));
    
    reader.setValue("foobar");
    char[] buf = new char[4];
    assertEquals(4, reader.read(buf));
    assertEquals("foob", new String(buf));
    assertEquals(2, reader.read(buf));
    assertEquals("ar", new String(buf, 0, 2));
    assertEquals(-1, reader.read(buf));
    reader.close();

    reader.setValue("foobar");
    assertEquals(0, reader.read(buf, 1, 0));
    assertEquals(3, reader.read(buf, 1, 3));
    assertEquals("foo", new String(buf, 1, 3));
    assertEquals(2, reader.read(CharBuffer.wrap(buf, 2, 2)));
    assertEquals("ba", new String(buf, 2, 2));
    assertEquals('r', (char) reader.read());
    assertEquals(-1, reader.read(buf));
    reader.close();

    reader.setValue("foobar");
    StringBuilder sb = new StringBuilder();
    int ch;
    while ((ch = reader.read()) != -1) {
      sb.append((char) ch);
    }
    reader.close();
    assertEquals("foobar", sb.toString());    
  }
  
}
