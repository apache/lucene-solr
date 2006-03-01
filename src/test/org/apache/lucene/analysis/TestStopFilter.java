package org.apache.lucene.analysis;

/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.io.StringReader;

import junit.framework.TestCase;

/**
 * @author yonik
 */
public class TestStopFilter extends TestCase {

  // other StopFilter functionality is already tested by TestStopAnalyzer

  public void testExactCase() throws IOException {
    StringReader reader = new StringReader("Now is The Time");
    String[] stopWords = new String[] { "is", "the", "Time" };
    TokenStream stream = new StopFilter(new WhitespaceTokenizer(reader), stopWords);
    assertEquals("Now", stream.next().termText());
    assertEquals("The", stream.next().termText());
    assertEquals(null, stream.next());
  }

  public void testIgnoreCase() throws IOException {
    StringReader reader = new StringReader("Now is The Time");
    String[] stopWords = new String[] { "is", "the", "Time" };
    TokenStream stream = new StopFilter(new WhitespaceTokenizer(reader), stopWords, true);
    assertEquals("Now", stream.next().termText());
    assertEquals(null,stream.next());
  }

}
