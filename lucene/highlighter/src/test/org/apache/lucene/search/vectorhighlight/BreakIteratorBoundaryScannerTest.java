package org.apache.lucene.search.vectorhighlight;

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

import java.text.BreakIterator;
import java.util.Locale;

import org.apache.lucene.util.LuceneTestCase;

public class BreakIteratorBoundaryScannerTest extends LuceneTestCase {
  static final String TEXT =
    "Apache Lucene(TM) is a high-performance, full-featured text search engine library written entirely in Java." +
    "\nIt is a technology suitable for nearly any application that requires\n" +
    "full-text search, especially cross-platform. \nApache Lucene is an open source project available for free download.";

  public void testOutOfRange() throws Exception {
    StringBuilder text = new StringBuilder(TEXT);
    BreakIterator bi = BreakIterator.getWordInstance(Locale.ROOT);
    BoundaryScanner scanner = new BreakIteratorBoundaryScanner(bi);
    
    int start = TEXT.length() + 1;
    assertEquals(start, scanner.findStartOffset(text, start));
    assertEquals(start, scanner.findEndOffset(text, start));
    start = 0;
    assertEquals(start, scanner.findStartOffset(text, start));
    start = -1;
    assertEquals(start, scanner.findEndOffset(text, start));
  }

  public void testWordBoundary() throws Exception {
    StringBuilder text = new StringBuilder(TEXT);
    BreakIterator bi = BreakIterator.getWordInstance(Locale.ROOT);
    BoundaryScanner scanner = new BreakIteratorBoundaryScanner(bi);
    
    int start = TEXT.indexOf("formance");
    int expected = TEXT.indexOf("high-performance");
    testFindStartOffset(text, start, expected, scanner);

    expected = TEXT.indexOf(", full");
    testFindEndOffset(text, start, expected, scanner);
  }

  public void testSentenceBoundary() throws Exception {
    StringBuilder text = new StringBuilder(TEXT);
    // we test this with default locale, its randomized by LuceneTestCase
    BreakIterator bi = BreakIterator.getSentenceInstance(Locale.getDefault());
    BoundaryScanner scanner = new BreakIteratorBoundaryScanner(bi);
    
    int start = TEXT.indexOf("any application");
    int expected = TEXT.indexOf("It is a");
    testFindStartOffset(text, start, expected, scanner);

    expected = TEXT.indexOf("Apache Lucene is an open source");
    testFindEndOffset(text, start, expected, scanner);
  }

  public void testLineBoundary() throws Exception {
    StringBuilder text = new StringBuilder(TEXT);
    // we test this with default locale, its randomized by LuceneTestCase
    BreakIterator bi = BreakIterator.getLineInstance(Locale.getDefault());
    BoundaryScanner scanner = new BreakIteratorBoundaryScanner(bi);
    
    int start = TEXT.indexOf("any application");
    int expected = TEXT.indexOf("nearly");
    testFindStartOffset(text, start, expected, scanner);

    expected = TEXT.indexOf("application that requires");
    testFindEndOffset(text, start, expected, scanner);
  }
  
  private void testFindStartOffset(StringBuilder text, int start, int expected, BoundaryScanner scanner) throws Exception {
    assertEquals(expected, scanner.findStartOffset(text, start));
  }
  
  private void testFindEndOffset(StringBuilder text, int start, int expected, BoundaryScanner scanner) throws Exception {
    assertEquals(expected, scanner.findEndOffset(text, start));
  }
}
