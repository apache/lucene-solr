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
package org.apache.lucene.util;


import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
    // The third instance is missing the TypeAttribute, so restoreState() should throw IllegalArgumentException
    expectThrows(IllegalArgumentException.class, () -> {
      src3.restoreState(state);
    });
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
    assertNotNull(flagsAtt2);
    final TypeAttribute typeAtt2 = clone.getAttribute(TypeAttribute.class);
    assertNotNull(typeAtt2);
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
    expectThrows(IllegalArgumentException.class, () -> {
      AttributeSource src = new AttributeSource();
      src.addAttribute(Token.class);
      fail("Should throw IllegalArgumentException");
    });
    
    expectThrows(IllegalArgumentException.class, () -> {
      AttributeSource src = new AttributeSource(Token.TOKEN_ATTRIBUTE_FACTORY);
      src.addAttribute(Token.class);
    });
    
    expectThrows(IllegalArgumentException.class, () -> {
      AttributeSource src = new AttributeSource();
      // break this by unsafe cast
      src.addAttribute((Class) Iterator.class);
    });
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
  
  public void testClonePayloadAttribute() throws Exception {
    // LUCENE-6055: verify that PayloadAttribute.clone() does deep cloning.
    PayloadAttributeImpl src = new PayloadAttributeImpl(new BytesRef(new byte[] { 1, 2, 3 }));
    
    // test clone()
    PayloadAttributeImpl clone = src.clone();
    clone.getPayload().bytes[0] = 10; // modify one byte, srcBytes shouldn't change
    assertEquals("clone() wasn't deep", 1, src.getPayload().bytes[0]);
    
    // test copyTo()
    clone = new PayloadAttributeImpl();
    src.copyTo(clone);
    clone.getPayload().bytes[0] = 10; // modify one byte, srcBytes shouldn't change
    assertEquals("clone() wasn't deep", 1, src.getPayload().bytes[0]);
  }

  public void testRemoveAllAttributes() {
    List<Class<? extends Attribute>> attrClasses = new ArrayList<>();
    attrClasses.add(CharTermAttribute.class);
    attrClasses.add(OffsetAttribute.class);
    attrClasses.add(FlagsAttribute.class);
    attrClasses.add(PayloadAttribute.class);
    attrClasses.add(PositionIncrementAttribute.class);
    attrClasses.add(TypeAttribute.class);

    // Add attributes with the default factory, then try to remove all of them
    AttributeSource defaultFactoryAttributeSource = new AttributeSource();

    assertFalse(defaultFactoryAttributeSource.hasAttributes());

    for (Class<? extends Attribute> attrClass : attrClasses) {
      defaultFactoryAttributeSource.addAttribute(attrClass);
      assertTrue("Missing added attribute " + attrClass.getSimpleName(),
          defaultFactoryAttributeSource.hasAttribute(attrClass));
    }

    defaultFactoryAttributeSource.removeAllAttributes();

    for (Class<? extends Attribute> attrClass : attrClasses) {
      assertFalse("Didn't remove attribute " + attrClass.getSimpleName(),
          defaultFactoryAttributeSource.hasAttribute(attrClass));
    }
    assertFalse(defaultFactoryAttributeSource.hasAttributes());

    // Add attributes with the packed implementations factory, then try to remove all of them
    AttributeSource packedImplsAttributeSource
        = new AttributeSource(TokenStream.DEFAULT_TOKEN_ATTRIBUTE_FACTORY);
    assertFalse(packedImplsAttributeSource.hasAttributes());

    for (Class<? extends Attribute> attrClass : attrClasses) {
      packedImplsAttributeSource.addAttribute(attrClass);
      assertTrue("Missing added attribute " + attrClass.getSimpleName(),
          packedImplsAttributeSource.hasAttribute(attrClass));
    }

    packedImplsAttributeSource.removeAllAttributes();

    for (Class<? extends Attribute> attrClass : attrClasses) {
      assertFalse("Didn't remove attribute " + attrClass.getSimpleName(),
          packedImplsAttributeSource.hasAttribute(attrClass));
    }
    assertFalse(packedImplsAttributeSource.hasAttributes());
  }
}
