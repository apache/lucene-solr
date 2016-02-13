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


import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.analysis.NumericTokenStream.NumericTermAttributeImpl;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.analysis.tokenattributes.TestCharTermAttributeImpl;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttributeImpl;

public class TestNumericTokenStream extends BaseTokenStreamTestCase {

  final long lvalue = random().nextLong();
  final int ivalue = random().nextInt();

  public void testLongStream() throws Exception {
    @SuppressWarnings("resource")
    final NumericTokenStream stream=new NumericTokenStream().setLongValue(lvalue);
    final TermToBytesRefAttribute bytesAtt = stream.getAttribute(TermToBytesRefAttribute.class);
    assertNotNull(bytesAtt);
    final TypeAttribute typeAtt = stream.getAttribute(TypeAttribute.class);
    assertNotNull(typeAtt);
    final NumericTokenStream.NumericTermAttribute numericAtt = stream.getAttribute(NumericTokenStream.NumericTermAttribute.class);
    assertNotNull(numericAtt);
    stream.reset();
    assertEquals(64, numericAtt.getValueSize());
    for (int shift=0; shift<64; shift+=NumericUtils.PRECISION_STEP_DEFAULT) {
      assertTrue("New token is available", stream.incrementToken());
      assertEquals("Shift value wrong", shift, numericAtt.getShift());
      assertEquals("Term is incorrectly encoded", lvalue & ~((1L << shift) - 1L), NumericUtils.prefixCodedToLong(bytesAtt.getBytesRef()));
      assertEquals("Term raw value is incorrectly encoded", lvalue & ~((1L << shift) - 1L), numericAtt.getRawValue());
      assertEquals("Type incorrect", (shift == 0) ? NumericTokenStream.TOKEN_TYPE_FULL_PREC : NumericTokenStream.TOKEN_TYPE_LOWER_PREC, typeAtt.type());
    }
    assertFalse("More tokens available", stream.incrementToken());
    stream.end();
    stream.close();
  }

  public void testIntStream() throws Exception {
    @SuppressWarnings("resource")
    final NumericTokenStream stream=new NumericTokenStream().setIntValue(ivalue);
    final TermToBytesRefAttribute bytesAtt = stream.getAttribute(TermToBytesRefAttribute.class);
    assertNotNull(bytesAtt);
    final TypeAttribute typeAtt = stream.getAttribute(TypeAttribute.class);
    assertNotNull(typeAtt);
    final NumericTokenStream.NumericTermAttribute numericAtt = stream.getAttribute(NumericTokenStream.NumericTermAttribute.class);
    assertNotNull(numericAtt);
    stream.reset();
    assertEquals(32, numericAtt.getValueSize());
    for (int shift=0; shift<32; shift+=NumericUtils.PRECISION_STEP_DEFAULT) {
      assertTrue("New token is available", stream.incrementToken());
      assertEquals("Shift value wrong", shift, numericAtt.getShift());
      assertEquals("Term is incorrectly encoded", ivalue & ~((1 << shift) - 1), NumericUtils.prefixCodedToInt(bytesAtt.getBytesRef()));
      assertEquals("Term raw value is incorrectly encoded", ((long) ivalue) & ~((1L << shift) - 1L), numericAtt.getRawValue());
      assertEquals("Type incorrect", (shift == 0) ? NumericTokenStream.TOKEN_TYPE_FULL_PREC : NumericTokenStream.TOKEN_TYPE_LOWER_PREC, typeAtt.type());
    }
    assertFalse("More tokens available", stream.incrementToken());
    stream.end();
    stream.close();
  }
  
  public void testNotInitialized() throws Exception {
    final NumericTokenStream stream=new NumericTokenStream();
    
    try {
      stream.reset();
      fail("reset() should not succeed.");
    } catch (IllegalStateException e) {
      // pass
    }

    try {
      stream.incrementToken();
      fail("incrementToken() should not succeed.");
    } catch (IllegalStateException e) {
      // pass
    }
    
    stream.close();
  }
  
  public static interface TestAttribute extends CharTermAttribute {}
  public static class TestAttributeImpl extends CharTermAttributeImpl implements TestAttribute {}
  
  public void testCTA() throws Exception {
    final NumericTokenStream stream=new NumericTokenStream();
    try {
      stream.addAttribute(CharTermAttribute.class);
      fail("Succeeded to add CharTermAttribute.");
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().startsWith("NumericTokenStream does not support"));
    }
    try {
      stream.addAttribute(TestAttribute.class);
      fail("Succeeded to add TestAttribute.");
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().startsWith("NumericTokenStream does not support"));
    }
    stream.close();
  }
  
  /** LUCENE-7027 */
  public void testCaptureStateAfterExhausted() throws Exception {
    // default precstep
    try (NumericTokenStream stream=new NumericTokenStream()) {
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
    try (NumericTokenStream stream=new NumericTokenStream(Integer.MAX_VALUE)) {
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
    NumericTermAttributeImpl att = new NumericTermAttributeImpl();
    att.init(lvalue, 64, 8, 0); // set some value, to make getBytesRef() work
    NumericTermAttributeImpl copy = TestCharTermAttributeImpl.assertCloneIsEqual(att);
    assertNotSame(att.getBytesRef(), copy.getBytesRef());
    NumericTermAttributeImpl copy2 = TestCharTermAttributeImpl.assertCopyIsEqual(att);
    assertNotSame(att.getBytesRef(), copy2.getBytesRef());
    
    // LUCENE-7027 test
    att.init(lvalue, 64, 8, 64); // Exhausted TokenStream -> should return empty BytesRef
    assertEquals(new BytesRef(), att.getBytesRef());
    copy = TestCharTermAttributeImpl.assertCloneIsEqual(att);
    assertEquals(new BytesRef(), copy.getBytesRef());
    assertNotSame(att.getBytesRef(), copy.getBytesRef());
    copy2 = TestCharTermAttributeImpl.assertCopyIsEqual(att);
    assertEquals(new BytesRef(), copy2.getBytesRef());
    assertNotSame(att.getBytesRef(), copy2.getBytesRef());
  }
  
}
