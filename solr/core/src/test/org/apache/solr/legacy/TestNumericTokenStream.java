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
package org.apache.solr.legacy;


import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.solr.legacy.LegacyNumericTokenStream.LegacyNumericTermAttributeImpl;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttributeImpl;

@Deprecated
public class TestNumericTokenStream extends BaseTokenStreamTestCase {

  final long lvalue = random().nextLong();
  final int ivalue = random().nextInt();

  public void testLongStream() throws Exception {
    @SuppressWarnings("resource")
    final LegacyNumericTokenStream stream=new LegacyNumericTokenStream().setLongValue(lvalue);
    final TermToBytesRefAttribute bytesAtt = stream.getAttribute(TermToBytesRefAttribute.class);
    assertNotNull(bytesAtt);
    final TypeAttribute typeAtt = stream.getAttribute(TypeAttribute.class);
    assertNotNull(typeAtt);
    final LegacyNumericTokenStream.LegacyNumericTermAttribute numericAtt = stream.getAttribute(LegacyNumericTokenStream.LegacyNumericTermAttribute.class);
    assertNotNull(numericAtt);
    stream.reset();
    assertEquals(64, numericAtt.getValueSize());
    for (int shift=0; shift<64; shift+= LegacyNumericUtils.PRECISION_STEP_DEFAULT) {
      assertTrue("New token is available", stream.incrementToken());
      assertEquals("Shift value wrong", shift, numericAtt.getShift());
      assertEquals("Term is incorrectly encoded", lvalue & ~((1L << shift) - 1L), LegacyNumericUtils.prefixCodedToLong(bytesAtt.getBytesRef()));
      assertEquals("Term raw value is incorrectly encoded", lvalue & ~((1L << shift) - 1L), numericAtt.getRawValue());
      assertEquals("Type incorrect", (shift == 0) ? LegacyNumericTokenStream.TOKEN_TYPE_FULL_PREC : LegacyNumericTokenStream.TOKEN_TYPE_LOWER_PREC, typeAtt.type());
    }
    assertFalse("More tokens available", stream.incrementToken());
    stream.end();
    stream.close();
  }

  public void testIntStream() throws Exception {
    @SuppressWarnings("resource")
    final LegacyNumericTokenStream stream=new LegacyNumericTokenStream().setIntValue(ivalue);
    final TermToBytesRefAttribute bytesAtt = stream.getAttribute(TermToBytesRefAttribute.class);
    assertNotNull(bytesAtt);
    final TypeAttribute typeAtt = stream.getAttribute(TypeAttribute.class);
    assertNotNull(typeAtt);
    final LegacyNumericTokenStream.LegacyNumericTermAttribute numericAtt = stream.getAttribute(LegacyNumericTokenStream.LegacyNumericTermAttribute.class);
    assertNotNull(numericAtt);
    stream.reset();
    assertEquals(32, numericAtt.getValueSize());
    for (int shift=0; shift<32; shift+= LegacyNumericUtils.PRECISION_STEP_DEFAULT) {
      assertTrue("New token is available", stream.incrementToken());
      assertEquals("Shift value wrong", shift, numericAtt.getShift());
      assertEquals("Term is incorrectly encoded", ivalue & ~((1 << shift) - 1), LegacyNumericUtils.prefixCodedToInt(bytesAtt.getBytesRef()));
      assertEquals("Term raw value is incorrectly encoded", ((long) ivalue) & ~((1L << shift) - 1L), numericAtt.getRawValue());
      assertEquals("Type incorrect", (shift == 0) ? LegacyNumericTokenStream.TOKEN_TYPE_FULL_PREC : LegacyNumericTokenStream.TOKEN_TYPE_LOWER_PREC, typeAtt.type());
    }
    assertFalse("More tokens available", stream.incrementToken());
    stream.end();
    stream.close();
  }
  
  public void testNotInitialized() throws Exception {
    final LegacyNumericTokenStream stream=new LegacyNumericTokenStream();
    
    expectThrows(IllegalStateException.class, () -> {
      stream.reset();
    });

    expectThrows(IllegalStateException.class, () -> {
      stream.incrementToken();
    });
    
    stream.close();
  }
  
  public static interface TestAttribute extends CharTermAttribute {}
  public static class TestAttributeImpl extends CharTermAttributeImpl implements TestAttribute {}
  
  public void testCTA() throws Exception {
    final LegacyNumericTokenStream stream=new LegacyNumericTokenStream();
    IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
      stream.addAttribute(CharTermAttribute.class);
    });
    assertTrue(e.getMessage().startsWith("LegacyNumericTokenStream does not support"));

    e = expectThrows(IllegalArgumentException.class, () -> {
      stream.addAttribute(TestAttribute.class);
    });
    assertTrue(e.getMessage().startsWith("LegacyNumericTokenStream does not support"));
    stream.close();
  }
  
  /** LUCENE-7027 */
  public void testCaptureStateAfterExhausted() throws Exception {
    // default precstep
    try (LegacyNumericTokenStream stream=new LegacyNumericTokenStream()) {
      // int
      stream.setIntValue(ivalue);
      stream.reset();
      while (stream.incrementToken());
      stream.captureState();
      stream.end();
      stream.captureState();
      // long
      stream.setLongValue(lvalue);
      stream.reset();
      while (stream.incrementToken());
      stream.captureState();
      stream.end();
      stream.captureState();
    }
    // huge precstep
    try (LegacyNumericTokenStream stream=new LegacyNumericTokenStream(Integer.MAX_VALUE)) {
      // int
      stream.setIntValue(ivalue);
      stream.reset();
      while (stream.incrementToken());
      stream.captureState();
      stream.end();
      stream.captureState();
      // long
      stream.setLongValue(lvalue);
      stream.reset();
      while (stream.incrementToken());
      stream.captureState();
      stream.end();
      stream.captureState();
    }
  }
  
  public void testAttributeClone() throws Exception {
    LegacyNumericTermAttributeImpl att = new LegacyNumericTermAttributeImpl();
    att.init(lvalue, 64, 8, 0); // set some value, to make getBytesRef() work
    LegacyNumericTermAttributeImpl copy = assertCloneIsEqual(att);
    assertNotSame(att.getBytesRef(), copy.getBytesRef());
    LegacyNumericTermAttributeImpl copy2 = assertCopyIsEqual(att);
    assertNotSame(att.getBytesRef(), copy2.getBytesRef());
    
    // LUCENE-7027 test
    att.init(lvalue, 64, 8, 64); // Exhausted TokenStream -> should return empty BytesRef
    assertEquals(new BytesRef(), att.getBytesRef());
    copy = assertCloneIsEqual(att);
    assertEquals(new BytesRef(), copy.getBytesRef());
    assertNotSame(att.getBytesRef(), copy.getBytesRef());
    copy2 = assertCopyIsEqual(att);
    assertEquals(new BytesRef(), copy2.getBytesRef());
    assertNotSame(att.getBytesRef(), copy2.getBytesRef());
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
