package org.apache.lucene.util;

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

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.tokenattributes.*;

import java.util.Iterator;

public class TestAttributeSource extends LuceneTestCase {

  public void testCaptureState() {
    // init a first instance
    AttributeSource src = new AttributeSource();
    CharTermAttribute termAtt = src.addAttribute(CharTermAttribute.class);
    TypeAttribute typeAtt = src.addAttribute(TypeAttribute.class);
    termAtt.append("TestTerm");
    typeAtt.setType("TestType");
    final int hashCode = src.hashCode();
    
    AttributeSource.State state = src.captureState();
    
    // modify the attributes
    termAtt.setEmpty().append("AnotherTestTerm");
    typeAtt.setType("AnotherTestType");
    assertTrue("Hash code should be different", hashCode != src.hashCode());
    
    src.restoreState(state);
    assertEquals("TestTerm", termAtt.toString());
    assertEquals("TestType", typeAtt.type());
    assertEquals("Hash code should be equal after restore", hashCode, src.hashCode());

    // restore into an exact configured copy
    AttributeSource copy = new AttributeSource();
    copy.addAttribute(CharTermAttribute.class);
    copy.addAttribute(TypeAttribute.class);
    copy.restoreState(state);
    assertEquals("Both AttributeSources should have same hashCode after restore", src.hashCode(), copy.hashCode());
    assertEquals("Both AttributeSources should be equal after restore", src, copy);
    
    // init a second instance (with attributes in different order and one additional attribute)
    AttributeSource src2 = new AttributeSource();
    typeAtt = src2.addAttribute(TypeAttribute.class);
    FlagsAttribute flagsAtt = src2.addAttribute(FlagsAttribute.class);
    termAtt = src2.addAttribute(CharTermAttribute.class);
    flagsAtt.setFlags(12345);

    src2.restoreState(state);
    assertEquals("TestTerm", termAtt.toString());
    assertEquals("TestType", typeAtt.type());
    assertEquals("FlagsAttribute should not be touched", 12345, flagsAtt.getFlags());

    // init a third instance missing one Attribute
    AttributeSource src3 = new AttributeSource();
    termAtt = src3.addAttribute(CharTermAttribute.class);
    try {
      src3.restoreState(state);
      fail("The third instance is missing the TypeAttribute, so restoreState() should throw IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
      // pass
    }
  }
  
  public void testCloneAttributes() {
    final AttributeSource src = new AttributeSource();
    final FlagsAttribute flagsAtt = src.addAttribute(FlagsAttribute.class);
    final TypeAttribute typeAtt = src.addAttribute(TypeAttribute.class);
    flagsAtt.setFlags(1234);
    typeAtt.setType("TestType");
    
    final AttributeSource clone = src.cloneAttributes();
    final Iterator<Class<? extends Attribute>> it = clone.getAttributeClassesIterator();
    assertEquals("FlagsAttribute must be the first attribute", FlagsAttribute.class, it.next());
    assertEquals("TypeAttribute must be the second attribute", TypeAttribute.class, it.next());
    assertFalse("No more attributes", it.hasNext());
    
    final FlagsAttribute flagsAtt2 = clone.getAttribute(FlagsAttribute.class);
    final TypeAttribute typeAtt2 = clone.getAttribute(TypeAttribute.class);
    assertNotSame("FlagsAttribute of original and clone must be different instances", flagsAtt2, flagsAtt);
    assertNotSame("TypeAttribute of original and clone must be different instances", typeAtt2, typeAtt);
    assertEquals("FlagsAttribute of original and clone must be equal", flagsAtt2, flagsAtt);
    assertEquals("TypeAttribute of original and clone must be equal", typeAtt2, typeAtt);
    
    // test copy back
    flagsAtt2.setFlags(4711);
    typeAtt2.setType("OtherType");
    clone.copyTo(src);
    assertEquals("FlagsAttribute of original must now contain updated term", 4711, flagsAtt.getFlags());
    assertEquals("TypeAttribute of original must now contain updated type", "OtherType", typeAtt.type());
    // verify again:
    assertNotSame("FlagsAttribute of original and clone must be different instances", flagsAtt2, flagsAtt);
    assertNotSame("TypeAttribute of original and clone must be different instances", typeAtt2, typeAtt);
    assertEquals("FlagsAttribute of original and clone must be equal", flagsAtt2, flagsAtt);
    assertEquals("TypeAttribute of original and clone must be equal", typeAtt2, typeAtt);
  }
  
  public void testDefaultAttributeFactory() throws Exception {
    AttributeSource src = new AttributeSource();
    
    assertTrue("CharTermAttribute is not implemented by CharTermAttributeImpl",
      src.addAttribute(CharTermAttribute.class) instanceof CharTermAttributeImpl);
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
  
  @SuppressWarnings({"rawtypes","unchecked"})
  public void testInvalidArguments() throws Exception {
    try {
      AttributeSource src = new AttributeSource();
      src.addAttribute(Token.class);
      fail("Should throw IllegalArgumentException");
    } catch (IllegalArgumentException iae) {}
    
    try {
      AttributeSource src = new AttributeSource(Token.TOKEN_ATTRIBUTE_FACTORY);
      src.addAttribute(Token.class);
      fail("Should throw IllegalArgumentException");
    } catch (IllegalArgumentException iae) {}
    
    try {
      AttributeSource src = new AttributeSource();
      // break this by unsafe cast
      src.addAttribute((Class) Iterator.class);
      fail("Should throw IllegalArgumentException");
    } catch (IllegalArgumentException iae) {}
  }
  
  public void testLUCENE_3042() throws Exception {
    final AttributeSource src1 = new AttributeSource();
    src1.addAttribute(CharTermAttribute.class).append("foo");
    int hash1 = src1.hashCode(); // this triggers a cached state
    final AttributeSource src2 = new AttributeSource(src1);
    src2.addAttribute(TypeAttribute.class).setType("bar");
    assertTrue("The hashCode is identical, so the captured state was preserved.", hash1 != src1.hashCode());
    assertEquals(src2.hashCode(), src1.hashCode());
  }
}
