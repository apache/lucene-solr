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
package org.apache.lucene.analysis.ja;

import java.io.IOException;

import org.apache.lucene.analysis.ja.util.CSVUtil;
import org.apache.lucene.util.LuceneTestCase;

/*
 * Tests for the CSVUtil class.
 */
public class TestCSVUtil extends LuceneTestCase {

  public void testQuoteEscapeQuotes() throws IOException {
    final String input = "\"Let It Be\" is a song and album by the The Beatles.";
    final String expectedOutput = input.replace("\"", "\"\"");
    implTestQuoteEscape(input, expectedOutput);
  }

  public void testQuoteEscapeComma() throws IOException {
    final String input = "To be, or not to be ...";
    final String expectedOutput = '"'+input+'"';
    implTestQuoteEscape(input, expectedOutput);
  }

  public void testQuoteEscapeQuotesAndComma() throws IOException {
    final String input = "\"To be, or not to be ...\" is a well-known phrase from Shakespeare's Hamlet.";
    final String expectedOutput = '"'+input.replace("\"", "\"\"")+'"';
    implTestQuoteEscape(input, expectedOutput);
  }

  private void implTestQuoteEscape(String input, String expectedOutput) throws IOException {
    final String actualOutput = CSVUtil.quoteEscape(input);
    assertEquals(expectedOutput, actualOutput);
  }

}
