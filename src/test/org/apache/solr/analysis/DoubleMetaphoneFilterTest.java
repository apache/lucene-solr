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
package org.apache.solr.analysis;

import junit.framework.TestCase;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.solr.analysis.BaseTokenTestCase.IterTokenStream;

public class DoubleMetaphoneFilterTest extends TestCase {

  public void testSize4FalseInject() throws Exception {
    TokenStream stream = new IterTokenStream("international");
    TokenStream filter = new DoubleMetaphoneFilter(stream, 4, false);

    Token token = filter.next(new Token());
    assertEquals(4, token.termLength());
    assertEquals("ANTR", new String(token.termBuffer(), 0, token.termLength()));

    assertNull(filter.next(new Token()));
  }

  public void testSize4TrueInject() throws Exception {
    TokenStream stream = new IterTokenStream("international");
    TokenStream filter = new DoubleMetaphoneFilter(stream, 4, true);

    Token token = filter.next(new Token());
    assertEquals(13, token.termLength());
    assertEquals("international", new String(token.termBuffer(), 0, token
        .termLength()));

    token = filter.next(new Token());
    assertEquals(4, token.termLength());
    assertEquals("ANTR", new String(token.termBuffer(), 0, token.termLength()));

    assertNull(filter.next(new Token()));
  }

  public void testAlternateInjectFalse() throws Exception {
    TokenStream stream = new IterTokenStream("Kuczewski");
    TokenStream filter = new DoubleMetaphoneFilter(stream, 4, false);

    Token token = filter.next(new Token());
    assertEquals(4, token.termLength());
    assertEquals("KSSK", new String(token.termBuffer(), 0, token.termLength()));

    token = filter.next(new Token());
    assertEquals(4, token.termLength());
    assertEquals("KXFS", new String(token.termBuffer(), 0, token.termLength()));
    assertNull(filter.next(new Token()));
  }

  public void testSize8FalseInject() throws Exception {
    TokenStream stream = new IterTokenStream("international");
    TokenStream filter = new DoubleMetaphoneFilter(stream, 8, false);

    Token token = filter.next(new Token());
    assertEquals(8, token.termLength());
    assertEquals("ANTRNXNL", new String(token.termBuffer(), 0, token
        .termLength()));

    assertNull(filter.next(new Token()));
  }

  public void testNonConvertableStringsWithInject() throws Exception {
    TokenStream stream = new IterTokenStream(
        new String[] { "12345", "#$%@#^%&" });
    TokenStream filter = new DoubleMetaphoneFilter(stream, 8, true);

    Token token = filter.next(new Token());
    assertEquals(5, token.termLength());
    assertEquals("12345", new String(token.termBuffer(), 0, token.termLength()));

    token = filter.next(new Token());
    assertEquals(8, token.termLength());
    assertEquals("#$%@#^%&", new String(token.termBuffer(), 0, token
        .termLength()));
  }

  public void testNonConvertableStringsWithoutInject() throws Exception {
    TokenStream stream = new IterTokenStream(
        new String[] { "12345", "#$%@#^%&" });
    TokenStream filter = new DoubleMetaphoneFilter(stream, 8, false);

    assertEquals("12345", filter.next(new Token()).term());
    
    // should have something after the stream
    stream = new IterTokenStream(
        new String[] { "12345", "#$%@#^%&", "hello" });
    filter = new DoubleMetaphoneFilter(stream, 8, false);
    assertNotNull(filter.next(new Token()));
  }

}
