package org.apache.lucene.analysis.tokenattributes;

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

public class TestTermAttributeImpl extends LuceneTestCase {

  public void testResize() {
    TermAttributeImpl t = new TermAttributeImpl();
    char[] content = "hello".toCharArray();
    t.setTermBuffer(content, 0, content.length);
    for (int i = 0; i < 2000; i++)
    {
      t.resizeTermBuffer(i);
      assertTrue(i <= t.termBuffer().length);
      assertEquals("hello", t.term());
    }
  }

  public void testGrow() {
    TermAttributeImpl t = new TermAttributeImpl();
    StringBuilder buf = new StringBuilder("ab");
    for (int i = 0; i < 20; i++)
    {
      char[] content = buf.toString().toCharArray();
      t.setTermBuffer(content, 0, content.length);
      assertEquals(buf.length(), t.termLength());
      assertEquals(buf.toString(), t.term());
      buf.append(buf.toString());
    }
    assertEquals(1048576, t.termLength());

    // now as a string, first variant
    t = new TermAttributeImpl();
    buf = new StringBuilder("ab");
    for (int i = 0; i < 20; i++)
    {
      String content = buf.toString();
      t.setTermBuffer(content, 0, content.length());
      assertEquals(content.length(), t.termLength());
      assertEquals(content, t.term());
      buf.append(content);
    }
    assertEquals(1048576, t.termLength());

    // now as a string, second variant
    t = new TermAttributeImpl();
    buf = new StringBuilder("ab");
    for (int i = 0; i < 20; i++)
    {
      String content = buf.toString();
      t.setTermBuffer(content);
      assertEquals(content.length(), t.termLength());
      assertEquals(content, t.term());
      buf.append(content);
    }
    assertEquals(1048576, t.termLength());

    // Test for slow growth to a long term
    t = new TermAttributeImpl();
    buf = new StringBuilder("a");
    for (int i = 0; i < 20000; i++)
    {
      String content = buf.toString();
      t.setTermBuffer(content);
      assertEquals(content.length(), t.termLength());
      assertEquals(content, t.term());
      buf.append("a");
    }
    assertEquals(20000, t.termLength());

    // Test for slow growth to a long term
    t = new TermAttributeImpl();
    buf = new StringBuilder("a");
    for (int i = 0; i < 20000; i++)
    {
      String content = buf.toString();
      t.setTermBuffer(content);
      assertEquals(content.length(), t.termLength());
      assertEquals(content, t.term());
      buf.append("a");
    }
    assertEquals(20000, t.termLength());
  }

  public void testToString() throws Exception {
    char[] b = {'a', 'l', 'o', 'h', 'a'};
    TermAttributeImpl t = new TermAttributeImpl();
    t.setTermBuffer(b, 0, 5);
    assertEquals("aloha", t.toString());

    t.setTermBuffer("hi there");
    assertEquals("hi there", t.toString());
  }

  public void testMixedStringArray() throws Exception {
    TermAttributeImpl t = new TermAttributeImpl();
    t.setTermBuffer("hello");
    assertEquals(t.termLength(), 5);
    assertEquals(t.term(), "hello");
    t.setTermBuffer("hello2");
    assertEquals(t.termLength(), 6);
    assertEquals(t.term(), "hello2");
    t.setTermBuffer("hello3".toCharArray(), 0, 6);
    assertEquals(t.term(), "hello3");

    // Make sure if we get the buffer and change a character
    // that term() reflects the change
    char[] buffer = t.termBuffer();
    buffer[1] = 'o';
    assertEquals(t.term(), "hollo3");
  }
  
  public void testClone() throws Exception {
    TermAttributeImpl t = new TermAttributeImpl();
    char[] content = "hello".toCharArray();
    t.setTermBuffer(content, 0, 5);
    char[] buf = t.termBuffer();
    TermAttributeImpl copy = (TermAttributeImpl) TestSimpleAttributeImpls.assertCloneIsEqual(t);
    assertEquals(t.term(), copy.term());
    assertNotSame(buf, copy.termBuffer());
  }
  
  public void testEquals() throws Exception {
    TermAttributeImpl t1a = new TermAttributeImpl();
    char[] content1a = "hello".toCharArray();
    t1a.setTermBuffer(content1a, 0, 5);
    TermAttributeImpl t1b = new TermAttributeImpl();
    char[] content1b = "hello".toCharArray();
    t1b.setTermBuffer(content1b, 0, 5);
    TermAttributeImpl t2 = new TermAttributeImpl();
    char[] content2 = "hello2".toCharArray();
    t2.setTermBuffer(content2, 0, 6);
    assertTrue(t1a.equals(t1b));
    assertFalse(t1a.equals(t2));
    assertFalse(t2.equals(t1b));
  }
  
  public void testCopyTo() throws Exception {
    TermAttributeImpl t = new TermAttributeImpl();
    TermAttributeImpl copy = (TermAttributeImpl) TestSimpleAttributeImpls.assertCopyIsEqual(t);
    assertEquals("", t.term());
    assertEquals("", copy.term());

    t = new TermAttributeImpl();
    char[] content = "hello".toCharArray();
    t.setTermBuffer(content, 0, 5);
    char[] buf = t.termBuffer();
    copy = (TermAttributeImpl) TestSimpleAttributeImpls.assertCopyIsEqual(t);
    assertEquals(t.term(), copy.term());
    assertNotSame(buf, copy.termBuffer());
  }
}
