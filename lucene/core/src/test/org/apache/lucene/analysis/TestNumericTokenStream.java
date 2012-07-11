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

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttributeImpl;

public class TestNumericTokenStream extends BaseTokenStreamTestCase {

  static final long lvalue = 4573245871874382L;
  static final int ivalue = 123456;

  public void testLongStream() throws Exception {
    final NumericTokenStream stream=new NumericTokenStream().setLongValue(lvalue);
    // use getAttribute to test if attributes really exist, if not an IAE will be throwed
    final TermToBytesRefAttribute bytesAtt = stream.getAttribute(TermToBytesRefAttribute.class);
    final TypeAttribute typeAtt = stream.getAttribute(TypeAttribute.class);
    final NumericTokenStream.NumericTermAttribute numericAtt = stream.getAttribute(NumericTokenStream.NumericTermAttribute.class);
    final BytesRef bytes = bytesAtt.getBytesRef();
    stream.reset();
    assertEquals(64, numericAtt.getValueSize());
    for (int shift=0; shift<64; shift+=NumericUtils.PRECISION_STEP_DEFAULT) {
      assertTrue("New token is available", stream.incrementToken());
      assertEquals("Shift value wrong", shift, numericAtt.getShift());
      final int hash = bytesAtt.fillBytesRef();
      assertEquals("Hash incorrect", bytes.hashCode(), hash);
      assertEquals("Term is incorrectly encoded", lvalue & ~((1L << shift) - 1L), NumericUtils.prefixCodedToLong(bytes));
      assertEquals("Term raw value is incorrectly encoded", lvalue & ~((1L << shift) - 1L), numericAtt.getRawValue());
      assertEquals("Type incorrect", (shift == 0) ? NumericTokenStream.TOKEN_TYPE_FULL_PREC : NumericTokenStream.TOKEN_TYPE_LOWER_PREC, typeAtt.type());
    }
    assertFalse("More tokens available", stream.incrementToken());
    stream.end();
    stream.close();
  }

  public void testIntStream() throws Exception {
    final NumericTokenStream stream=new NumericTokenStream().setIntValue(ivalue);
    // use getAttribute to test if attributes really exist, if not an IAE will be throwed
    final TermToBytesRefAttribute bytesAtt = stream.getAttribute(TermToBytesRefAttribute.class);
    final TypeAttribute typeAtt = stream.getAttribute(TypeAttribute.class);
    final NumericTokenStream.NumericTermAttribute numericAtt = stream.getAttribute(NumericTokenStream.NumericTermAttribute.class);
    final BytesRef bytes = bytesAtt.getBytesRef();
    stream.reset();
    assertEquals(32, numericAtt.getValueSize());
    for (int shift=0; shift<32; shift+=NumericUtils.PRECISION_STEP_DEFAULT) {
      assertTrue("New token is available", stream.incrementToken());
      assertEquals("Shift value wrong", shift, numericAtt.getShift());
      final int hash = bytesAtt.fillBytesRef();
      assertEquals("Hash incorrect", bytes.hashCode(), hash);
      assertEquals("Term is incorrectly encoded", ivalue & ~((1 << shift) - 1), NumericUtils.prefixCodedToInt(bytes));
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
  }
  
}
