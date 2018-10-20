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
package org.apache.lucene.analysis.miscellaneous;


import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenStream;
import org.junit.Test;

/**
 * Test the truncate token filter.
 */
public class TestTruncateTokenFilter extends BaseTokenStreamTestCase {

  public void testTruncating() throws Exception {
    TokenStream stream = whitespaceMockTokenizer("abcdefg 1234567 ABCDEFG abcde abc 12345 123");
    stream = new TruncateTokenFilter(stream, 5);
    assertTokenStreamContents(stream, new String[]{"abcde", "12345", "ABCDE", "abcde", "abc", "12345", "123"});
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNonPositiveLength() throws Exception {
    new TruncateTokenFilter(whitespaceMockTokenizer("length must be a positive number"), -48);
  }
}
