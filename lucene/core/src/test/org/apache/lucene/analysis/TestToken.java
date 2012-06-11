package org.apache.lucene.analysis;

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

import org.apache.lucene.analysis.tokenattributes.*;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.Attribute;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util._TestUtil;

import java.io.StringReader;
import java.util.HashMap;

public class TestToken extends LuceneTestCase {

  public void testCtor() throws Exception {
    Token t = new Token();
    char[] content = "hello".toCharArray();
    t.copyBuffer(content, 0, content.length);
    assertNotSame(t.buffer(), content);
    assertEquals(0, t.startOffset());
    assertEquals(0, t.endOffset());
    assertEquals("hello", t.toString());
    assertEquals("word", t.type());
    assertEquals(0, t.getFlags());

    t = new Token(6, 22);
    t.copyBuffer(content, 0, content.length);
    assertEquals("hello", t.toString());
    assertEquals("hello", t.toString());
    assertEquals(6, t.startOffset());
    assertEquals(22, t.endOffset());
    assertEquals("word", t.type());
    assertEquals(0, t.getFlags());

    t = new Token(6, 22, 7);
    t.copyBuffer(content, 0, content.length);
    assertEquals("hello", t.toString());
    assertEquals("hello", t.toString());
    assertEquals(6, t.startOffset());
    assertEquals(22, t.endOffset());
    assertEquals("word", t.type());
    assertEquals(7, t.getFlags());

    t = new Token(6, 22, "junk");
    t.copyBuffer(content, 0, content.length);
    assertEquals("hello", t.toString());
    assertEquals("hello", t.toString());
    assertEquals(6, t.startOffset());
    assertEquals(22, t.endOffset());
    assertEquals("junk", t.type());
    assertEquals(0, t.getFlags());
  }

  public void testResize() {
    Token t = new Token();
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
    Token t = new Token();
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

    // now as a string, second variant
    t = new Token();
    buf = new StringBuilder("ab");
    for (int i = 0; i < 20; i++)
    {
      t.setEmpty().append(buf);
      String content = buf.toString();
      assertEquals(content.length(), t.length());
      assertEquals(content, t.toString());
      buf.append(content);
    }
    assertEquals(1048576, t.length());

    // Test for slow growth to a long term
    t = new Token();
    buf = new StringBuilder("a");
    for (int i = 0; i < 20000; i++)
    {
      t.setEmpty().append(buf);
      String content = buf.toString();
      assertEquals(content.length(), t.length());
      assertEquals(content, t.toString());
      buf.append("a");
    }
    assertEquals(20000, t.length());

    // Test for slow growth to a long term
    t = new Token();
    buf = new StringBuilder("a");
    for (int i = 0; i < 20000; i++)
    {
      t.setEmpty().append(buf);
      String content = buf.toString();
      assertEquals(content.length(), t.length());
      assertEquals(content, t.toString());
      buf.append("a");
    }
    assertEquals(20000, t.length());
  }

  public void testToString() throws Exception {
    char[] b = {'a', 'l', 'o', 'h', 'a'};
    Token t = new Token("", 0, 5);
    t.copyBuffer(b, 0, 5);
    assertEquals("aloha", t.toString());

    t.setEmpty().append("hi there");
    assertEquals("hi there", t.toString());
  }

  public void testTermBufferEquals() throws Exception {
    Token t1a = new Token();
    char[] content1a = "hello".toCharArray();
    t1a.copyBuffer(content1a, 0, 5);
    Token t1b = new Token();
    char[] content1b = "hello".toCharArray();
    t1b.copyBuffer(content1b, 0, 5);
    Token t2 = new Token();
    char[] content2 = "hello2".toCharArray();
    t2.copyBuffer(content2, 0, 6);
    assertTrue(t1a.equals(t1b));
    assertFalse(t1a.equals(t2));
    assertFalse(t2.equals(t1b));
  }
  
  public void testMixedStringArray() throws Exception {
    Token t = new Token("hello", 0, 5);
    assertEquals(t.length(), 5);
    assertEquals(t.toString(), "hello");
    t.setEmpty().append("hello2");
    assertEquals(t.length(), 6);
    assertEquals(t.toString(), "hello2");
    t.copyBuffer("hello3".toCharArray(), 0, 6);
    assertEquals(t.toString(), "hello3");

    char[] buffer = t.buffer();
    buffer[1] = 'o';
    assertEquals(t.toString(), "hollo3");
  }
  
  public void testClone() throws Exception {
    Token t = new Token(0, 5);
    char[] content = "hello".toCharArray();
    t.copyBuffer(content, 0, 5);
    char[] buf = t.buffer();
    Token copy = assertCloneIsEqual(t);
    assertEquals(t.toString(), copy.toString());
    assertNotSame(buf, copy.buffer());

    BytesRef pl = new BytesRef(new byte[]{1,2,3,4});
    t.setPayload(pl);
    copy = assertCloneIsEqual(t);
    assertEquals(pl, copy.getPayload());
    assertNotSame(pl, copy.getPayload());
  }
  
  public void testCopyTo() throws Exception {
    Token t = new Token();
    Token copy = assertCopyIsEqual(t);
    assertEquals("", t.toString());
    assertEquals("", copy.toString());

    t = new Token(0, 5);
    char[] content = "hello".toCharArray();
    t.copyBuffer(content, 0, 5);
    char[] buf = t.buffer();
    copy = assertCopyIsEqual(t);
    assertEquals(t.toString(), copy.toString());
    assertNotSame(buf, copy.buffer());

    BytesRef pl = new BytesRef(new byte[]{1,2,3,4});
    t.setPayload(pl);
    copy = assertCopyIsEqual(t);
    assertEquals(pl, copy.getPayload());
    assertNotSame(pl, copy.getPayload());
  }
  
  public interface SenselessAttribute extends Attribute {}
  
  public static final class SenselessAttributeImpl extends AttributeImpl implements SenselessAttribute {
    @Override
    public void copyTo(AttributeImpl target) {}
    @Override
    public void clear() {}
    @Override
    public boolean equals(Object o) { return (o instanceof SenselessAttributeImpl); }
    @Override
    public int hashCode() { return 0; }
  }

  public void testTokenAttributeFactory() throws Exception {
    TokenStream ts = new MockTokenizer(Token.TOKEN_ATTRIBUTE_FACTORY, new StringReader("foo bar"), MockTokenizer.WHITESPACE, false, MockTokenizer.DEFAULT_MAX_TOKEN_LENGTH);
    
    assertTrue("SenselessAttribute is not implemented by SenselessAttributeImpl",
      ts.addAttribute(SenselessAttribute.class) instanceof SenselessAttributeImpl);
    
    assertTrue("CharTermAttribute is not implemented by Token",
      ts.addAttribute(CharTermAttribute.class) instanceof Token);
    assertTrue("OffsetAttribute is not implemented by Token",
      ts.addAttribute(OffsetAttribute.class) instanceof Token);
    assertTrue("FlagsAttribute is not implemented by Token",
      ts.addAttribute(FlagsAttribute.class) instanceof Token);
    assertTrue("PayloadAttribute is not implemented by Token",
      ts.addAttribute(PayloadAttribute.class) instanceof Token);
    assertTrue("PositionIncrementAttribute is not implemented by Token", 
      ts.addAttribute(PositionIncrementAttribute.class) instanceof Token);
    assertTrue("TypeAttribute is not implemented by Token",
      ts.addAttribute(TypeAttribute.class) instanceof Token);
  }

  public void testAttributeReflection() throws Exception {
    Token t = new Token("foobar", 6, 22, 8);
    _TestUtil.assertAttributeReflection(t,
      new HashMap<String,Object>() {{
        put(CharTermAttribute.class.getName() + "#term", "foobar");
        put(TermToBytesRefAttribute.class.getName() + "#bytes", new BytesRef("foobar"));
        put(OffsetAttribute.class.getName() + "#startOffset", 6);
        put(OffsetAttribute.class.getName() + "#endOffset", 22);
        put(PositionIncrementAttribute.class.getName() + "#positionIncrement", 1);
        put(PayloadAttribute.class.getName() + "#payload", null);
        put(TypeAttribute.class.getName() + "#type", TypeAttribute.DEFAULT_TYPE);
        put(FlagsAttribute.class.getName() + "#flags", 8);
      }});
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
}
