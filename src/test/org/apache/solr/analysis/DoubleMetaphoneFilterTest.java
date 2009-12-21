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

import java.io.StringReader;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceTokenizer;

public class DoubleMetaphoneFilterTest extends BaseTokenTestCase {

  public void testSize4FalseInject() throws Exception {
    TokenStream stream = new WhitespaceTokenizer(new StringReader("international"));
    TokenStream filter = new DoubleMetaphoneFilter(stream, 4, false);
    assertTokenStreamContents(filter, new String[] { "ANTR" });
  }

  public void testSize4TrueInject() throws Exception {
    TokenStream stream = new WhitespaceTokenizer(new StringReader("international"));
    TokenStream filter = new DoubleMetaphoneFilter(stream, 4, true);
    assertTokenStreamContents(filter, new String[] { "international", "ANTR" });
  }

  public void testAlternateInjectFalse() throws Exception {
    TokenStream stream = new WhitespaceTokenizer(new StringReader("Kuczewski"));
    TokenStream filter = new DoubleMetaphoneFilter(stream, 4, false);
    assertTokenStreamContents(filter, new String[] { "KSSK", "KXFS" });
  }

  public void testSize8FalseInject() throws Exception {
    TokenStream stream = new WhitespaceTokenizer(new StringReader("international"));
    TokenStream filter = new DoubleMetaphoneFilter(stream, 8, false);
    assertTokenStreamContents(filter, new String[] { "ANTRNXNL" });
  }

  public void testNonConvertableStringsWithInject() throws Exception {
    TokenStream stream = new WhitespaceTokenizer(new StringReader("12345 #$%@#^%&"));
    TokenStream filter = new DoubleMetaphoneFilter(stream, 8, true);
    assertTokenStreamContents(filter, new String[] { "12345", "#$%@#^%&" });
  }

  public void testNonConvertableStringsWithoutInject() throws Exception {
    TokenStream stream = new WhitespaceTokenizer(new StringReader("12345 #$%@#^%&"));
    TokenStream filter = new DoubleMetaphoneFilter(stream, 8, false);
    assertTokenStreamContents(filter, new String[] { "12345", "#$%@#^%&" });
    
    // should have something after the stream
    stream = new WhitespaceTokenizer(new StringReader("12345 #$%@#^%& hello"));
    filter = new DoubleMetaphoneFilter(stream, 8, false);
    assertTokenStreamContents(filter, new String[] { "12345", "#$%@#^%&", "HL" });
  }

}
