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

package org.apache.lucene.analysis.reverse;

import java.io.StringReader;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceTokenizer;
import org.apache.lucene.util.LuceneTestCase;

public class TestReverseStringFilter extends LuceneTestCase {
  public void testFilter() throws Exception {
    TokenStream stream = new WhitespaceTokenizer(
        new StringReader("Do have a nice day"));     // 1-4 length string
    ReverseStringFilter filter = new ReverseStringFilter(stream);
    final Token reusableToken = new Token();
    assertEquals("oD", filter.next(reusableToken).term());
    assertEquals("evah", filter.next(reusableToken).term());
    assertEquals("a", filter.next(reusableToken).term());
    assertEquals("ecin", filter.next(reusableToken).term());
    assertEquals("yad", filter.next(reusableToken).term());
    assertNull(filter.next(reusableToken));
  }

  public void testReverseString() throws Exception {
    assertEquals( "A", ReverseStringFilter.reverse( "A" ) );
    assertEquals( "BA", ReverseStringFilter.reverse( "AB" ) );
    assertEquals( "CBA", ReverseStringFilter.reverse( "ABC" ) );
  }
  
  public void testReverseChar() throws Exception {
    char[] buffer = { 'A', 'B', 'C', 'D', 'E', 'F' };
    ReverseStringFilter.reverse( buffer, 2, 3 );
    assertEquals( "ABEDCF", new String( buffer ) );
  }
}
