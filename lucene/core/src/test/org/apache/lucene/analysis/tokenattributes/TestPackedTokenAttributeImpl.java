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


import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.TestUtil;

import java.io.StringReader;
import java.util.HashMap;

public class TestPackedTokenAttributeImpl extends LuceneTestCase {

  /* the CharTermAttributeStuff is tested by TestCharTermAttributeImpl */
  
  public void testClone() throws Exception {
    PackedTokenAttributeImpl t = new PackedTokenAttributeImpl();
    t.setOffset(0, 5);
    char[] content = "hello".toCharArray();
    t.copyBuffer(content, 0, 5);
    char[] buf = t.buffer();
    PackedTokenAttributeImpl copy = TestCharTermAttributeImpl.assertCloneIsEqual(t);
    assertEquals(t.toString(), copy.toString());
    assertNotSame(buf, copy.buffer());
  }
  
  public void testCopyTo() throws Exception {
    PackedTokenAttributeImpl t = new PackedTokenAttributeImpl();
    PackedTokenAttributeImpl copy = TestCharTermAttributeImpl.assertCopyIsEqual(t);
    assertEquals("", t.toString());
    assertEquals("", copy.toString());

    t = new PackedTokenAttributeImpl();
    t.setOffset(0, 5);
    char[] content = "hello".toCharArray();
    t.copyBuffer(content, 0, 5);
    char[] buf = t.buffer();
    copy = TestCharTermAttributeImpl.assertCopyIsEqual(t);
    assertEquals(t.toString(), copy.toString());
    assertNotSame(buf, copy.buffer());
  }
  
  public void testPackedTokenAttributeFactory() throws Exception {
    TokenStream ts = new MockTokenizer(TokenStream.DEFAULT_TOKEN_ATTRIBUTE_FACTORY, MockTokenizer.WHITESPACE, false, MockTokenizer.DEFAULT_MAX_TOKEN_LENGTH);
    ((Tokenizer)ts).setReader(new StringReader("foo bar"));
    
    assertTrue("CharTermAttribute is not implemented by Token",
      ts.addAttribute(CharTermAttribute.class) instanceof PackedTokenAttributeImpl);
    assertTrue("OffsetAttribute is not implemented by Token",
      ts.addAttribute(OffsetAttribute.class) instanceof PackedTokenAttributeImpl);
    assertTrue("PositionIncrementAttribute is not implemented by Token", 
      ts.addAttribute(PositionIncrementAttribute.class) instanceof PackedTokenAttributeImpl);
    assertTrue("TypeAttribute is not implemented by Token",
      ts.addAttribute(TypeAttribute.class) instanceof PackedTokenAttributeImpl);

    assertTrue("FlagsAttribute is not implemented by FlagsAttributeImpl",
        ts.addAttribute(FlagsAttribute.class) instanceof FlagsAttributeImpl);  
  }

  public void testAttributeReflection() throws Exception {
    PackedTokenAttributeImpl t = new PackedTokenAttributeImpl();
    t.append("foobar");
    t.setOffset(6,  22);
    t.setPositionIncrement(3);
    t.setPositionLength(11);
    t.setType("foobar");
    t.setTermFrequency(42);
    TestUtil.assertAttributeReflection(t,
        new HashMap<String, Object>() {{
          put(CharTermAttribute.class.getName() + "#term", "foobar");
          put(TermToBytesRefAttribute.class.getName() + "#bytes", new BytesRef("foobar"));
          put(OffsetAttribute.class.getName() + "#startOffset", 6);
          put(OffsetAttribute.class.getName() + "#endOffset", 22);
          put(PositionIncrementAttribute.class.getName() + "#positionIncrement", 3);
          put(PositionLengthAttribute.class.getName() + "#positionLength", 11);
          put(TypeAttribute.class.getName() + "#type", "foobar");
          put(TermFrequencyAttribute.class.getName() + "#termFrequency", 42);
        }});
  }
}
