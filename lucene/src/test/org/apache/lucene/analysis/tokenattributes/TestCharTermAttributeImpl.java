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
import java.nio.CharBuffer;
import java.util.Formatter;
import java.util.Locale;
import java.util.regex.Pattern;

public class TestCharTermAttributeImpl extends LuceneTestCase {

  public TestCharTermAttributeImpl(String name) {
    super(name);
  }

  public void testResize() {
    CharTermAttributeImpl t = new CharTermAttributeImpl();
    char[] content = "hello".toCharArray();
    t.copyBuffer(content, 0, content.length);
    for (int i = 0; i < 2000; i++)
    {
      t.resizeBuffer(i);
      assertTrue(i <= t.buffer().length);
      assertEquals("hello", t.toString());
    }
  }

  public void testGrow() {
    CharTermAttributeImpl t = new CharTermAttributeImpl();
    StringBuilder buf = new StringBuilder("ab");
    for (int i = 0; i < 20; i++)
    {
      char[] content = buf.toString().toCharArray();
      t.copyBuffer(content, 0, content.length);
      assertEquals(buf.length(), t.length());
      assertEquals(buf.toString(), t.toString());
      buf.append(buf.toString());
    }
    assertEquals(1048576, t.length());

    // now as a StringBuilder, first variant
    t = new CharTermAttributeImpl();
    buf = new StringBuilder("ab");
    for (int i = 0; i < 20; i++)
    {
      t.setEmpty().append(buf);
      assertEquals(buf.length(), t.length());
      assertEquals(buf.toString(), t.toString());
      buf.append(t);
    }
    assertEquals(1048576, t.length());

    // Test for slow growth to a long term
    t = new CharTermAttributeImpl();
    buf = new StringBuilder("a");
    for (int i = 0; i < 20000; i++)
    {
      t.setEmpty().append(buf);
      assertEquals(buf.length(), t.length());
      assertEquals(buf.toString(), t.toString());
      buf.append("a");
    }
    assertEquals(20000, t.length());
  }

  public void testToString() throws Exception {
    char[] b = {'a', 'l', 'o', 'h', 'a'};
    CharTermAttributeImpl t = new CharTermAttributeImpl();
    t.copyBuffer(b, 0, 5);
    assertEquals("aloha", t.toString());

    t.setEmpty().append("hi there");
    assertEquals("hi there", t.toString());
  }

  public void testClone() throws Exception {
    CharTermAttributeImpl t = new CharTermAttributeImpl();
    char[] content = "hello".toCharArray();
    t.copyBuffer(content, 0, 5);
    char[] buf = t.buffer();
    CharTermAttributeImpl copy = (CharTermAttributeImpl) TestSimpleAttributeImpls.assertCloneIsEqual(t);
    assertEquals(t.toString(), copy.toString());
    assertNotSame(buf, copy.buffer());
  }
  
  public void testEquals() throws Exception {
    CharTermAttributeImpl t1a = new CharTermAttributeImpl();
    char[] content1a = "hello".toCharArray();
    t1a.copyBuffer(content1a, 0, 5);
    CharTermAttributeImpl t1b = new CharTermAttributeImpl();
    char[] content1b = "hello".toCharArray();
    t1b.copyBuffer(content1b, 0, 5);
    CharTermAttributeImpl t2 = new CharTermAttributeImpl();
    char[] content2 = "hello2".toCharArray();
    t2.copyBuffer(content2, 0, 6);
    assertTrue(t1a.equals(t1b));
    assertFalse(t1a.equals(t2));
    assertFalse(t2.equals(t1b));
  }
  
  public void testCopyTo() throws Exception {
    CharTermAttributeImpl t = new CharTermAttributeImpl();
    CharTermAttributeImpl copy = (CharTermAttributeImpl) TestSimpleAttributeImpls.assertCopyIsEqual(t);
    assertEquals("", t.toString());
    assertEquals("", copy.toString());

    t = new CharTermAttributeImpl();
    char[] content = "hello".toCharArray();
    t.copyBuffer(content, 0, 5);
    char[] buf = t.buffer();
    copy = (CharTermAttributeImpl) TestSimpleAttributeImpls.assertCopyIsEqual(t);
    assertEquals(t.toString(), copy.toString());
    assertNotSame(buf, copy.buffer());
  }
  
  public void testCharSequenceInterface() {
    final String s = "0123456789"; 
    final CharTermAttributeImpl t = new CharTermAttributeImpl();
    t.append(s);
    
    assertEquals(s.length(), t.length());
    assertEquals("12", t.subSequence(1,3).toString());
    assertEquals(s, t.subSequence(0,s.length()).toString());
    
    assertTrue(Pattern.matches("01\\d+", t));
    assertTrue(Pattern.matches("34", t.subSequence(3,5)));
    
    assertEquals(s.subSequence(3,7).toString(), t.subSequence(3,7).toString());
    
    for (int i = 0; i < s.length(); i++) {
      assertTrue(t.charAt(i) == s.charAt(i));
    }
  }

  public void testAppendableInterface() {
    CharTermAttributeImpl t = new CharTermAttributeImpl();
    Formatter formatter = new Formatter(t, Locale.US);
    formatter.format("%d", 1234);
    assertEquals("1234", t.toString());
    formatter.format("%d", 5678);
    assertEquals("12345678", t.toString());
    t.append('9');
    assertEquals("123456789", t.toString());
    t.append("0");
    assertEquals("1234567890", t.toString());
    t.append("0123456789", 1, 3);
    assertEquals("123456789012", t.toString());
    t.append(CharBuffer.wrap("0123456789".toCharArray()), 3, 5);
    assertEquals("12345678901234", t.toString());
    t.append(t);
    assertEquals("1234567890123412345678901234", t.toString());
    t.append(new StringBuilder("0123456789"), 5, 7);
    assertEquals("123456789012341234567890123456", t.toString());
    t.append(new StringBuffer(t));
    assertEquals("123456789012341234567890123456123456789012341234567890123456", t.toString());
    // very wierd, to test if a subSlice is wrapped correct :)
    t.setEmpty().append(CharBuffer.wrap("0123456789".toCharArray(), 3, 5) /* "34" */, 1, 2);
    assertEquals("4", t.toString());
  }
  
}
