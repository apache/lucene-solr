package org.apache.lucene.analysis;

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

import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;

public class TestNumericTokenStream extends BaseTokenStreamTestCase {

  static final long lvalue = 4573245871874382L;
  static final int ivalue = 123456;

  public void testLongStream() throws Exception {
    final NumericTokenStream stream=new NumericTokenStream().setLongValue(lvalue);
    // use getAttribute to test if attributes really exist, if not an IAE will be throwed
    final CharTermAttribute termAtt = stream.getAttribute(CharTermAttribute.class);
    final TypeAttribute typeAtt = stream.getAttribute(TypeAttribute.class);
    for (int shift=0; shift<64; shift+=NumericUtils.PRECISION_STEP_DEFAULT) {
      assertTrue("New token is available", stream.incrementToken());
      assertEquals("Term is correctly encoded", NumericUtils.longToPrefixCoded(lvalue, shift), termAtt.toString());
      assertEquals("Type correct", (shift == 0) ? NumericTokenStream.TOKEN_TYPE_FULL_PREC : NumericTokenStream.TOKEN_TYPE_LOWER_PREC, typeAtt.type());
    }
    assertFalse("No more tokens available", stream.incrementToken());
  }

  public void testIntStream() throws Exception {
    final NumericTokenStream stream=new NumericTokenStream().setIntValue(ivalue);
    // use getAttribute to test if attributes really exist, if not an IAE will be throwed
    final CharTermAttribute termAtt = stream.getAttribute(CharTermAttribute.class);
    final TypeAttribute typeAtt = stream.getAttribute(TypeAttribute.class);
    for (int shift=0; shift<32; shift+=NumericUtils.PRECISION_STEP_DEFAULT) {
      assertTrue("New token is available", stream.incrementToken());
      assertEquals("Term is correctly encoded", NumericUtils.intToPrefixCoded(ivalue, shift), termAtt.toString());
      assertEquals("Type correct", (shift == 0) ? NumericTokenStream.TOKEN_TYPE_FULL_PREC : NumericTokenStream.TOKEN_TYPE_LOWER_PREC, typeAtt.type());
    }
    assertFalse("No more tokens available", stream.incrementToken());
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
  
}
