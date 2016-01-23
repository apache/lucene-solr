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

import org.apache.lucene.util.LegacyNumericUtils;
import org.apache.lucene.analysis.LegacyNumericTokenStream.LegacyNumericTermAttributeImpl;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.analysis.tokenattributes.TestCharTermAttributeImpl;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttributeImpl;

public class TestNumericTokenStream extends BaseTokenStreamTestCase {

  static final long lvalue = 4573245871874382L;
  static final int ivalue = 123456;

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
    final LegacyNumericTokenStream stream=new LegacyNumericTokenStream();
    try {
      stream.addAttribute(CharTermAttribute.class);
      fail("Succeeded to add CharTermAttribute.");
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().startsWith("LegacyNumericTokenStream does not support"));
    }
    try {
      stream.addAttribute(TestAttribute.class);
      fail("Succeeded to add TestAttribute.");
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().startsWith("LegacyNumericTokenStream does not support"));
    }
    stream.close();
  }
  
  public void testAttributeClone() throws Exception {
    LegacyNumericTermAttributeImpl att = new LegacyNumericTermAttributeImpl();
    att.init(1234L, 64, 8, 0); // set some value, to make getBytesRef() work
    LegacyNumericTermAttributeImpl copy = TestCharTermAttributeImpl.assertCloneIsEqual(att);
    assertNotSame(att.getBytesRef(), copy.getBytesRef());
    LegacyNumericTermAttributeImpl copy2 = TestCharTermAttributeImpl.assertCopyIsEqual(att);
    assertNotSame(att.getBytesRef(), copy2.getBytesRef());
  }
  
}
