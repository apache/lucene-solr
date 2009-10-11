package org.apache.lucene.util;

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

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.tokenattributes.*;

import java.util.Iterator;

public class TestAttributeSource extends LuceneTestCase {

  public void testCaptureState() {
    // init a first instance
    AttributeSource src = new AttributeSource();
    TermAttribute termAtt = src.addAttribute(TermAttribute.class);
    TypeAttribute typeAtt = src.addAttribute(TypeAttribute.class);
    termAtt.setTermBuffer("TestTerm");
    typeAtt.setType("TestType");
    final int hashCode = src.hashCode();
    
    AttributeSource.State state = src.captureState();
    
    // modify the attributes
    termAtt.setTermBuffer("AnotherTestTerm");
    typeAtt.setType("AnotherTestType");
    assertTrue("Hash code should be different", hashCode != src.hashCode());
    
    src.restoreState(state);
    assertEquals("TestTerm", termAtt.term());
    assertEquals("TestType", typeAtt.type());
    assertEquals("Hash code should be equal after restore", hashCode, src.hashCode());

    // restore into an exact configured copy
    AttributeSource copy = new AttributeSource();
    copy.addAttribute(TermAttribute.class);
    copy.addAttribute(TypeAttribute.class);
    copy.restoreState(state);
    assertEquals("Both AttributeSources should have same hashCode after restore", src.hashCode(), copy.hashCode());
    assertEquals("Both AttributeSources should be equal after restore", src, copy);
    
    // init a second instance (with attributes in different order and one additional attribute)
    AttributeSource src2 = new AttributeSource();
    typeAtt = src2.addAttribute(TypeAttribute.class);
    FlagsAttribute flagsAtt = src2.addAttribute(FlagsAttribute.class);
    termAtt = src2.addAttribute(TermAttribute.class);
    flagsAtt.setFlags(12345);

    src2.restoreState(state);
    assertEquals("TestTerm", termAtt.term());
    assertEquals("TestType", typeAtt.type());
    assertEquals("FlagsAttribute should not be touched", 12345, flagsAtt.getFlags());

    // init a third instance missing one Attribute
    AttributeSource src3 = new AttributeSource();
    termAtt = src3.addAttribute(TermAttribute.class);
    try {
      src3.restoreState(state);
      fail("The third instance is missing the TypeAttribute, so restoreState() should throw IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
      // pass
    }
  }
  
  public void testCloneAttributes() {
    final AttributeSource src = new AttributeSource();
    final TermAttribute termAtt = src.addAttribute(TermAttribute.class);
    final TypeAttribute typeAtt = src.addAttribute(TypeAttribute.class);
    termAtt.setTermBuffer("TestTerm");
    typeAtt.setType("TestType");
    
    final AttributeSource clone = src.cloneAttributes();
    final Iterator<Class<? extends Attribute>> it = clone.getAttributeClassesIterator();
    assertEquals("TermAttribute must be the first attribute", TermAttribute.class, it.next());
    assertEquals("TypeAttribute must be the second attribute", TypeAttribute.class, it.next());
    assertFalse("No more attributes", it.hasNext());
    
    final TermAttribute termAtt2 = clone.getAttribute(TermAttribute.class);
    final TypeAttribute typeAtt2 = clone.getAttribute(TypeAttribute.class);
    assertNotSame("TermAttribute of original and clone must be different instances", termAtt2, termAtt);
    assertNotSame("TypeAttribute of original and clone must be different instances", typeAtt2, typeAtt);
    assertEquals("TermAttribute of original and clone must be equal", termAtt2, termAtt);
    assertEquals("TypeAttribute of original and clone must be equal", typeAtt2, typeAtt);
  }
  
  public void testToStringAndMultiAttributeImplementations() {
    AttributeSource src = new AttributeSource();
    TermAttribute termAtt = src.addAttribute(TermAttribute.class);
    TypeAttribute typeAtt = src.addAttribute(TypeAttribute.class);
    termAtt.setTermBuffer("TestTerm");
    typeAtt.setType("TestType");    
    assertEquals("Attributes should appear in original order", "("+termAtt.toString()+","+typeAtt.toString()+")", src.toString());
    Iterator<AttributeImpl> it = src.getAttributeImplsIterator();
    assertTrue("Iterator should have 2 attributes left", it.hasNext());
    assertSame("First AttributeImpl from iterator should be termAtt", termAtt, it.next());
    assertTrue("Iterator should have 1 attributes left", it.hasNext());
    assertSame("Second AttributeImpl from iterator should be typeAtt", typeAtt, it.next());
    assertFalse("Iterator should have 0 attributes left", it.hasNext());

    src = new AttributeSource();
    src.addAttributeImpl(new Token());
    // this should not add a new attribute as Token implements TermAttribute, too
    termAtt = src.addAttribute(TermAttribute.class);
    assertTrue("TermAttribute should be implemented by Token", termAtt instanceof Token);
    // get the Token attribute and check, that it is the only one
    it = src.getAttributeImplsIterator();
    Token tok = (Token) it.next();
    assertFalse("There should be only one attribute implementation instance", it.hasNext());
    
    termAtt.setTermBuffer("TestTerm");
    assertEquals("Token should only printed once", "("+tok.toString()+")", src.toString());
  }
  
  public void testDefaultAttributeFactory() throws Exception {
    AttributeSource src = new AttributeSource();
    
    assertTrue("TermAttribute is not implemented by TermAttributeImpl",
      src.addAttribute(TermAttribute.class) instanceof TermAttributeImpl);
    assertTrue("OffsetAttribute is not implemented by OffsetAttributeImpl",
      src.addAttribute(OffsetAttribute.class) instanceof OffsetAttributeImpl);
    assertTrue("FlagsAttribute is not implemented by FlagsAttributeImpl",
      src.addAttribute(FlagsAttribute.class) instanceof FlagsAttributeImpl);
    assertTrue("PayloadAttribute is not implemented by PayloadAttributeImpl",
      src.addAttribute(PayloadAttribute.class) instanceof PayloadAttributeImpl);
    assertTrue("PositionIncrementAttribute is not implemented by PositionIncrementAttributeImpl", 
      src.addAttribute(PositionIncrementAttribute.class) instanceof PositionIncrementAttributeImpl);
    assertTrue("TypeAttribute is not implemented by TypeAttributeImpl",
      src.addAttribute(TypeAttribute.class) instanceof TypeAttributeImpl);
  }
}
