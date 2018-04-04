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


import org.apache.lucene.analysis.tokenattributes.*;
import org.apache.lucene.util.AttributeReflector;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.Attribute;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.TestUtil;

import java.io.StringReader;
import java.util.HashMap;

@Deprecated
public class TestToken extends LuceneTestCase {

  public void testCtor() throws Exception {
    Token t = new Token("hello", 0, 0);
    assertEquals(0, t.startOffset());
    assertEquals(0, t.endOffset());
    assertEquals(1, t.getPositionIncrement());
    assertEquals(1, t.getPositionLength());
    assertEquals("hello", t.toString());
    assertEquals("word", t.type());
    assertEquals(0, t.getFlags());
    assertNull(t.getPayload());
  }
  
  /* the CharTermAttributeStuff is tested by TestCharTermAttributeImpl */

  public void testClone() throws Exception {
    Token t = new Token();
    t.setOffset(0, 5);
    char[] content = "hello".toCharArray();
    t.copyBuffer(content, 0, 5);
    char[] buf = t.buffer();
    Token copy = TestCharTermAttributeImpl.assertCloneIsEqual(t);
    assertEquals(t.toString(), copy.toString());
    assertNotSame(buf, copy.buffer());

    BytesRef pl = new BytesRef(new byte[]{1,2,3,4});
    t.setPayload(pl);
    copy = TestCharTermAttributeImpl.assertCloneIsEqual(t);
    assertEquals(pl, copy.getPayload());
    assertNotSame(pl, copy.getPayload());
  }
  
  public void testCopyTo() throws Exception {
    Token t = new Token();
    Token copy = TestCharTermAttributeImpl.assertCopyIsEqual(t);
    assertEquals("", t.toString());
    assertEquals("", copy.toString());

    t = new Token();
    t.setOffset(0, 5);
    char[] content = "hello".toCharArray();
    t.copyBuffer(content, 0, 5);
    char[] buf = t.buffer();
    copy = TestCharTermAttributeImpl.assertCopyIsEqual(t);
    assertEquals(t.toString(), copy.toString());
    assertNotSame(buf, copy.buffer());

    BytesRef pl = new BytesRef(new byte[]{1,2,3,4});
    t.setPayload(pl);
    copy = TestCharTermAttributeImpl.assertCopyIsEqual(t);
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
    @Override
    public void reflectWith(AttributeReflector reflector) {}
  }

  public void testTokenAttributeFactory() throws Exception {
    TokenStream ts = new MockTokenizer(Token.TOKEN_ATTRIBUTE_FACTORY, MockTokenizer.WHITESPACE, false, MockTokenizer.DEFAULT_MAX_TOKEN_LENGTH);
    ((Tokenizer)ts).setReader(new StringReader("foo bar"));
    
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
    Token t = new Token("foobar", 6, 22);
    t.setFlags(8);
    t.setPositionIncrement(3);
    t.setPositionLength(11);
    t.setTermFrequency(42);
    TestUtil.assertAttributeReflection(t,
        new HashMap<String, Object>() {{
          put(CharTermAttribute.class.getName() + "#term", "foobar");
          put(TermToBytesRefAttribute.class.getName() + "#bytes", new BytesRef("foobar"));
          put(OffsetAttribute.class.getName() + "#startOffset", 6);
          put(OffsetAttribute.class.getName() + "#endOffset", 22);
          put(PositionIncrementAttribute.class.getName() + "#positionIncrement", 3);
          put(PositionLengthAttribute.class.getName() + "#positionLength", 11);
          put(PayloadAttribute.class.getName() + "#payload", null);
          put(TypeAttribute.class.getName() + "#type", TypeAttribute.DEFAULT_TYPE);
          put(FlagsAttribute.class.getName() + "#flags", 8);
          put(TermFrequencyAttribute.class.getName() + "#termFrequency", 42);
        }});
  }
}
