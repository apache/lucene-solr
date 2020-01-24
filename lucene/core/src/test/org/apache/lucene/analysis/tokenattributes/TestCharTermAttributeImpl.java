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
package org.apache.lucene.analysis.tokenattributes;


import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.TestUtil;

import java.nio.CharBuffer;
import java.util.HashMap;
import java.util.Formatter;
import java.util.Locale;
import java.util.regex.Pattern;

public class TestCharTermAttributeImpl extends LuceneTestCase {

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

  public void testSetLength() {
    CharTermAttributeImpl t = new CharTermAttributeImpl();
    char[] content = "hello".toCharArray();
    t.copyBuffer(content, 0, content.length);
    expectThrows(IndexOutOfBoundsException.class, () -> {
      t.setLength(-1);
    });
  }

  @Slow
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
    CharTermAttributeImpl copy = assertCloneIsEqual(t);
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
    CharTermAttributeImpl copy = assertCopyIsEqual(t);
    assertEquals("", t.toString());
    assertEquals("", copy.toString());

    t = new CharTermAttributeImpl();
    char[] content = "hello".toCharArray();
    t.copyBuffer(content, 0, 5);
    char[] buf = t.buffer();
    copy = assertCopyIsEqual(t);
    assertEquals(t.toString(), copy.toString());
    assertNotSame(buf, copy.buffer());
  }
  
  public void testAttributeReflection() throws Exception {
    CharTermAttributeImpl t = new CharTermAttributeImpl();
    t.append("foobar");
    TestUtil.assertAttributeReflection(t, new HashMap<String, Object>() {{
      put(CharTermAttribute.class.getName() + "#term", "foobar");
      put(TermToBytesRefAttribute.class.getName() + "#bytes", new BytesRef("foobar"));
    }});
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
    Formatter formatter = new Formatter(t, Locale.ROOT);
    formatter.format("%d", 1234);
    assertEquals("1234", t.toString());
    formatter.format("%d", 5678);
    assertEquals("12345678", t.toString());
    t.append('9');
    assertEquals("123456789", t.toString());
    t.append((CharSequence) "0");
    assertEquals("1234567890", t.toString());
    t.append((CharSequence) "0123456789", 1, 3);
    assertEquals("123456789012", t.toString());
    t.append((CharSequence) CharBuffer.wrap("0123456789".toCharArray()), 3, 5);
    assertEquals("12345678901234", t.toString());
    t.append((CharSequence) t);
    assertEquals("1234567890123412345678901234", t.toString());
    t.append((CharSequence) new StringBuilder("0123456789"), 5, 7);
    assertEquals("123456789012341234567890123456", t.toString());
    t.append((CharSequence) new StringBuffer(t));
    assertEquals("123456789012341234567890123456123456789012341234567890123456", t.toString());
    // very wierd, to test if a subSlice is wrapped correct :)
    CharBuffer buf = CharBuffer.wrap("0123456789".toCharArray(), 3, 5);
    assertEquals("34567", buf.toString());
    t.setEmpty().append((CharSequence) buf, 1, 2);
    assertEquals("4", t.toString());
    CharTermAttribute t2 = new CharTermAttributeImpl();
    t2.append("test");
    t.append((CharSequence) t2);
    assertEquals("4test", t.toString());
    t.append((CharSequence) t2, 1, 2);
    assertEquals("4teste", t.toString());
    
    expectThrows(IndexOutOfBoundsException.class, () -> {
      t.append((CharSequence) t2, 1, 5);
    });

    expectThrows(IndexOutOfBoundsException.class, () -> {
      t.append((CharSequence) t2, 1, 0);
    });
    
    t.append((CharSequence) null);
    assertEquals("4testenull", t.toString());
  }
  
  public void testAppendableInterfaceWithLongSequences() {
    CharTermAttributeImpl t = new CharTermAttributeImpl();
    t.append((CharSequence) "01234567890123456789012345678901234567890123456789");
    t.append((CharSequence) CharBuffer.wrap("01234567890123456789012345678901234567890123456789".toCharArray()), 3, 50);
    assertEquals("0123456789012345678901234567890123456789012345678934567890123456789012345678901234567890123456789", t.toString());
    t.setEmpty().append((CharSequence) new StringBuilder("01234567890123456789"), 5, 17);
    assertEquals((CharSequence) "567890123456", t.toString());
    t.append(new StringBuffer(t));
    assertEquals((CharSequence) "567890123456567890123456", t.toString());
    // very wierd, to test if a subSlice is wrapped correct :)
    CharBuffer buf = CharBuffer.wrap("012345678901234567890123456789".toCharArray(), 3, 15);
    assertEquals("345678901234567", buf.toString());
    t.setEmpty().append(buf, 1, 14);
    assertEquals("4567890123456", t.toString());
    
    // finally use a completely custom CharSequence that is not catched by instanceof checks
    final String longTestString = "012345678901234567890123456789";
    t.append(new CharSequence() {
      @Override
      public char charAt(int i) { return longTestString.charAt(i); }
      @Override
      public int length() { return longTestString.length(); }
      @Override
      public CharSequence subSequence(int start, int end) { return longTestString.subSequence(start, end); }
      @Override
      public String toString() { return longTestString; }
    });
    assertEquals("4567890123456"+longTestString, t.toString());
  }
  
  public void testNonCharSequenceAppend() {
    CharTermAttributeImpl t = new CharTermAttributeImpl();
    t.append("0123456789");
    t.append("0123456789");
    assertEquals("01234567890123456789", t.toString());
    t.append(new StringBuilder("0123456789"));
    assertEquals("012345678901234567890123456789", t.toString());
    CharTermAttribute t2 = new CharTermAttributeImpl();
    t2.append("test");
    t.append(t2);
    assertEquals("012345678901234567890123456789test", t.toString());
    t.append((String) null);
    t.append((StringBuilder) null);
    t.append((CharTermAttribute) null);
    assertEquals("012345678901234567890123456789testnullnullnull", t.toString());
  }
  
  public void testExceptions() {
    CharTermAttributeImpl t = new CharTermAttributeImpl();
    t.append("test");
    assertEquals("test", t.toString());

    expectThrows(IndexOutOfBoundsException.class, () -> {
      t.charAt(-1);
    });

    expectThrows(IndexOutOfBoundsException.class, () -> {
      t.charAt(4);
    });

    expectThrows(IndexOutOfBoundsException.class, () -> {
      t.subSequence(0, 5);
    });

    expectThrows(IndexOutOfBoundsException.class, () -> {
      t.subSequence(5, 0);
    });
  }

  public static <T extends AttributeImpl> T assertCloneIsEqual(T att) {
    @SuppressWarnings("unchecked")
    T clone = (T) att.clone();
    assertEquals("Clone must be equal", att, clone);
    assertEquals("Clone's hashcode must be equal", att.hashCode(), clone.hashCode());
    return clone;
  }

  public static <T extends AttributeImpl> T assertCopyIsEqual(T att) throws Exception {
    @SuppressWarnings("unchecked")
    T copy = (T) att.getClass().newInstance();
    att.copyTo(copy);
    assertEquals("Copied instance must be equal", att, copy);
    assertEquals("Copied instance's hashcode must be equal", att.hashCode(), copy.hashCode());
    return copy;
  }
  
  /*
  
  // test speed of the dynamic instanceof checks in append(CharSequence),
  // to find the best max length for the generic while (start<end) loop:
  public void testAppendPerf() {
    CharTermAttributeImpl t = new CharTermAttributeImpl();
    final int count = 32;
    CharSequence[] csq = new CharSequence[count * 6];
    final StringBuilder sb = new StringBuilder();
    for (int i=0,j=0; i<count; i++) {
      sb.append(i%10);
      final String testString = sb.toString();
      CharTermAttribute cta = new CharTermAttributeImpl();
      cta.append(testString);
      csq[j++] = cta;
      csq[j++] = testString;
      csq[j++] = new StringBuilder(sb);
      csq[j++] = new StringBuffer(sb);
      csq[j++] = CharBuffer.wrap(testString.toCharArray());
      csq[j++] = new CharSequence() {
        public char charAt(int i) { return testString.charAt(i); }
        public int length() { return testString.length(); }
        public CharSequence subSequence(int start, int end) { return testString.subSequence(start, end); }
        public String toString() { return testString; }
      };
    }

    Random rnd = newRandom();
    long startTime = System.currentTimeMillis();
    for (int i=0; i<100000000; i++) {
      t.setEmpty().append(csq[rnd.nextInt(csq.length)]);
    }
    long endTime = System.currentTimeMillis();
    System.out.println("Time: " + (endTime-startTime)/1000.0 + " s");
  }
  
  */

}
