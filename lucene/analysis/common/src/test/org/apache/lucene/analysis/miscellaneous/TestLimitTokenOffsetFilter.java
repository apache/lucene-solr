package org.apache.lucene.analysis.miscellaneous;

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

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.junit.Test;

public class TestLimitTokenOffsetFilter extends BaseTokenStreamTestCase {

  public void test() throws Exception {
    for (final boolean consumeAll : new boolean[]{true, false}) {
      MockTokenizer tokenizer = whitespaceMockTokenizer("A1 B2 C3 D4 E5 F6");
      tokenizer.setEnableChecks(consumeAll);
      //note with '3', this test would fail if erroneously the filter used endOffset instead
      TokenStream stream = new LimitTokenOffsetFilter(tokenizer, 3, consumeAll);
      assertTokenStreamContents(stream, new String[]{"A1", "B2"});
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIllegalArguments() throws Exception {
    new LimitTokenOffsetFilter(whitespaceMockTokenizer("A1 B2 C3 D4 E5 F6"), -1);
  }
}
