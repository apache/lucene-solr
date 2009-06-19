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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

public class TestNumericTokenStream extends LuceneTestCase {

  static final int precisionStep = 8;
  static final long lvalue = 4573245871874382L;
  static final int ivalue = 123456;

  public void testLongStreamNewAPI() throws Exception {
    final NumericTokenStream stream=new NumericTokenStream(precisionStep).setLongValue(lvalue);
    stream.setUseNewAPI(true);
    // use getAttribute to test if attributes really exist, if not an IAE will be throwed
    final TermAttribute termAtt = (TermAttribute) stream.getAttribute(TermAttribute.class);
    for (int shift=0; shift<64; shift+=precisionStep) {
      assertTrue("New token is available", stream.incrementToken());
      assertEquals("Term is correctly encoded", NumericUtils.longToPrefixCoded(lvalue, shift), termAtt.term());
    }
    assertFalse("No more tokens available", stream.incrementToken());
  }
  
  public void testLongStreamOldAPI() throws Exception {
    final NumericTokenStream stream=new NumericTokenStream(precisionStep).setLongValue(lvalue);
    stream.setUseNewAPI(false);
    Token tok=new Token();
    for (int shift=0; shift<64; shift+=precisionStep) {
      assertNotNull("New token is available", tok=stream.next(tok));
      assertEquals("Term is correctly encoded", NumericUtils.longToPrefixCoded(lvalue, shift), tok.term());
    }
    assertNull("No more tokens available", stream.next(tok));
  }

  public void testIntStreamNewAPI() throws Exception {
    final NumericTokenStream stream=new NumericTokenStream(precisionStep).setIntValue(ivalue);
    stream.setUseNewAPI(true);
    // use getAttribute to test if attributes really exist, if not an IAE will be throwed
    final TermAttribute termAtt = (TermAttribute) stream.getAttribute(TermAttribute.class);
    for (int shift=0; shift<32; shift+=precisionStep) {
      assertTrue("New token is available", stream.incrementToken());
      assertEquals("Term is correctly encoded", NumericUtils.intToPrefixCoded(ivalue, shift), termAtt.term());
    }
    assertFalse("No more tokens available", stream.incrementToken());
  }
  
  public void testIntStreamOldAPI() throws Exception {
    final NumericTokenStream stream=new NumericTokenStream(precisionStep).setIntValue(ivalue);
    stream.setUseNewAPI(false);
    Token tok=new Token();
    for (int shift=0; shift<32; shift+=precisionStep) {
      assertNotNull("New token is available", tok=stream.next(tok));
      assertEquals("Term is correctly encoded", NumericUtils.intToPrefixCoded(ivalue, shift), tok.term());
    }
    assertNull("No more tokens available", stream.next(tok));
  }
  
  public void testNotInitialized() throws Exception {
    final NumericTokenStream stream=new NumericTokenStream(precisionStep);
    
    try {
      stream.reset();
      fail("reset() should not succeed.");
    } catch (IllegalStateException e) {
      // pass
    }

    stream.setUseNewAPI(true);
    try {
      stream.incrementToken();
      fail("incrementToken() should not succeed.");
    } catch (IllegalStateException e) {
      // pass
    }

    stream.setUseNewAPI(false);
    try {
      stream.next(new Token());
      fail("next() should not succeed.");
    } catch (IllegalStateException e) {
      // pass
    }
  }
  
}
