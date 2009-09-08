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

import org.apache.lucene.index.Payload;
import org.apache.lucene.analysis.tokenattributes.TestSimpleAttributeImpls;
import org.apache.lucene.util.LuceneTestCase;

public class TestToken extends LuceneTestCase {

  public TestToken(String name) {
    super(name);
  }

  public void testCtor() throws Exception {
    Token t = new Token();
    char[] content = "hello".toCharArray();
    t.setTermBuffer(content, 0, content.length);
    char[] buf = t.termBuffer();
    assertNotSame(t.termBuffer(), content);
    assertEquals("hello", t.term());
    assertEquals("word", t.type());
    assertEquals(0, t.getFlags());

    t = new Token(6, 22);
    t.setTermBuffer(content, 0, content.length);
    assertEquals("hello", t.term());
    assertEquals("(hello,6,22)", t.toString());
    assertEquals("word", t.type());
    assertEquals(0, t.getFlags());

    t = new Token(6, 22, 7);
    t.setTermBuffer(content, 0, content.length);
    assertEquals("hello", t.term());
    assertEquals("(hello,6,22)", t.toString());
    assertEquals(7, t.getFlags());

    t = new Token(6, 22, "junk");
    t.setTermBuffer(content, 0, content.length);
    assertEquals("hello", t.term());
    assertEquals("(hello,6,22,type=junk)", t.toString());
    assertEquals(0, t.getFlags());
  }

  public void testResize() {
    Token t = new Token();
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
    Token t = new Token();
    StringBuffer buf = new StringBuffer("ab");
    for (int i = 0; i < 20; i++)
    {
      char[] content = buf.toString().toCharArray();
      t.setTermBuffer(content, 0, content.length);
      assertEquals(buf.length(), t.termLength());
      assertEquals(buf.toString(), t.term());
      buf.append(buf.toString());
    }
    assertEquals(1048576, t.termLength());
    assertEquals(1179654, t.termBuffer().length);

    // now as a string, first variant
    t = new Token();
    buf = new StringBuffer("ab");
    for (int i = 0; i < 20; i++)
    {
      String content = buf.toString();
      t.setTermBuffer(content, 0, content.length());
      assertEquals(content.length(), t.termLength());
      assertEquals(content, t.term());
      buf.append(content);
    }
    assertEquals(1048576, t.termLength());
    assertEquals(1179654, t.termBuffer().length);

    // now as a string, second variant
    t = new Token();
    buf = new StringBuffer("ab");
    for (int i = 0; i < 20; i++)
    {
      String content = buf.toString();
      t.setTermBuffer(content);
      assertEquals(content.length(), t.termLength());
      assertEquals(content, t.term());
      buf.append(content);
    }
    assertEquals(1048576, t.termLength());
    assertEquals(1179654, t.termBuffer().length);

    // Test for slow growth to a long term
    t = new Token();
    buf = new StringBuffer("a");
    for (int i = 0; i < 20000; i++)
    {
      String content = buf.toString();
      t.setTermBuffer(content);
      assertEquals(content.length(), t.termLength());
      assertEquals(content, t.term());
      buf.append("a");
    }
    assertEquals(20000, t.termLength());
    assertEquals(20167, t.termBuffer().length);

    // Test for slow growth to a long term
    t = new Token();
    buf = new StringBuffer("a");
    for (int i = 0; i < 20000; i++)
    {
      String content = buf.toString();
      t.setTermBuffer(content);
      assertEquals(content.length(), t.termLength());
      assertEquals(content, t.term());
      buf.append("a");
    }
    assertEquals(20000, t.termLength());
    assertEquals(20167, t.termBuffer().length);
  }

  public void testToString() throws Exception {
    char[] b = {'a', 'l', 'o', 'h', 'a'};
    Token t = new Token("", 0, 5);
    t.setTermBuffer(b, 0, 5);
    assertEquals("(aloha,0,5)", t.toString());

    t.setTermText("hi there");
    assertEquals("(hi there,0,5)", t.toString());
  }

  public void testTermBufferEquals() throws Exception {
    Token t1a = new Token();
    char[] content1a = "hello".toCharArray();
    t1a.setTermBuffer(content1a, 0, 5);
    Token t1b = new Token();
    char[] content1b = "hello".toCharArray();
    t1b.setTermBuffer(content1b, 0, 5);
    Token t2 = new Token();
    char[] content2 = "hello2".toCharArray();
    t2.setTermBuffer(content2, 0, 6);
    assertTrue(t1a.equals(t1b));
    assertFalse(t1a.equals(t2));
    assertFalse(t2.equals(t1b));
  }
  
  public void testMixedStringArray() throws Exception {
    Token t = new Token("hello", 0, 5);
    assertEquals(t.termText(), "hello");
    assertEquals(t.termLength(), 5);
    assertEquals(t.term(), "hello");
    t.setTermText("hello2");
    assertEquals(t.termLength(), 6);
    assertEquals(t.term(), "hello2");
    t.setTermBuffer("hello3".toCharArray(), 0, 6);
    assertEquals(t.termText(), "hello3");

    // Make sure if we get the buffer and change a character
    // that termText() reflects the change
    char[] buffer = t.termBuffer();
    buffer[1] = 'o';
    assertEquals(t.termText(), "hollo3");
  }
  
  public void testClone() throws Exception {
    Token t = new Token(0, 5);
    char[] content = "hello".toCharArray();
    t.setTermBuffer(content, 0, 5);
    char[] buf = t.termBuffer();
    Token copy = (Token) TestSimpleAttributeImpls.assertCloneIsEqual(t);
    assertEquals(t.term(), copy.term());
    assertNotSame(buf, copy.termBuffer());

    Payload pl = new Payload(new byte[]{1,2,3,4});
    t.setPayload(pl);
    copy = (Token) TestSimpleAttributeImpls.assertCloneIsEqual(t);
    assertEquals(pl, copy.getPayload());
    assertNotSame(pl, copy.getPayload());
  }
  
  public void testCopyTo() throws Exception {
    Token t = new Token();
    Token copy = (Token) TestSimpleAttributeImpls.assertCopyIsEqual(t);
    assertEquals("", t.term());
    assertEquals("", copy.term());

    t = new Token(0, 5);
    char[] content = "hello".toCharArray();
    t.setTermBuffer(content, 0, 5);
    char[] buf = t.termBuffer();
    copy = (Token) TestSimpleAttributeImpls.assertCopyIsEqual(t);
    assertEquals(t.term(), copy.term());
    assertNotSame(buf, copy.termBuffer());

    Payload pl = new Payload(new byte[]{1,2,3,4});
    t.setPayload(pl);
    copy = (Token) TestSimpleAttributeImpls.assertCopyIsEqual(t);
    assertEquals(pl, copy.getPayload());
    assertNotSame(pl, copy.getPayload());
  }
}
