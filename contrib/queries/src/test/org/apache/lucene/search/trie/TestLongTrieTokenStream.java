package org.apache.lucene.search.trie;

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
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

public class TestLongTrieTokenStream extends LuceneTestCase {

  static final int precisionStep = 8;
  static final long value = 4573245871874382L;

  public void testStreamNewAPI() throws Exception {
    final LongTrieTokenStream stream=new LongTrieTokenStream(value, precisionStep);
    stream.setUseNewAPI(true);
    final ShiftAttribute shiftAtt = (ShiftAttribute) stream.addAttribute(ShiftAttribute.class);
    final TermAttribute termAtt = (TermAttribute) stream.addAttribute(TermAttribute.class);
    for (int shift=0; shift<64; shift+=precisionStep) {
      assertTrue("New token is available", stream.incrementToken());
      assertEquals("Shift value", shift, shiftAtt.getShift());
      assertEquals("Term is correctly encoded", TrieUtils.longToPrefixCoded(value, shift), termAtt.term());
    }
    assertFalse("No more tokens available", stream.incrementToken());
  }
  
  public void testStreamOldAPI() throws Exception {
    final LongTrieTokenStream stream=new LongTrieTokenStream(value, precisionStep);
    stream.setUseNewAPI(false);
    Token tok=new Token();
    for (int shift=0; shift<64; shift+=precisionStep) {
      assertNotNull("New token is available", tok=stream.next(tok));
      assertEquals("Term is correctly encoded", TrieUtils.longToPrefixCoded(value, shift), tok.term());
    }
    assertNull("No more tokens available", stream.next(tok));
  }
  
}
