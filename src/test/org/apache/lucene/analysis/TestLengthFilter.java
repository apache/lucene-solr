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

import java.io.StringReader;

public class TestLengthFilter extends LuceneTestCase {
  
  public void testFilter() throws Exception {
    TokenStream stream = new WhitespaceTokenizer(
        new StringReader("short toolong evenmuchlongertext a ab toolong foo"));
    LengthFilter filter = new LengthFilter(stream, 2, 6);
    final Token reusableToken = new Token();
    assertEquals("short", filter.next(reusableToken).term());
    assertEquals("ab", filter.next(reusableToken).term());
    assertEquals("foo", filter.next(reusableToken).term());
    assertNull(filter.next(reusableToken));
  }

}
