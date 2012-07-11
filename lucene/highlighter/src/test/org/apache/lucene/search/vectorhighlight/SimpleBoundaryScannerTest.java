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

import org.apache.lucene.util.LuceneTestCase;

public class SimpleBoundaryScannerTest extends LuceneTestCase {
  static final String TEXT =
    "Apache Lucene(TM) is a high-performance, full-featured\ntext search engine library written entirely in Java.";

  public void testFindStartOffset() throws Exception {
    StringBuilder text = new StringBuilder(TEXT);
    BoundaryScanner scanner = new SimpleBoundaryScanner();
    
    // test out of range
    int start = TEXT.length() + 1;
    assertEquals(start, scanner.findStartOffset(text, start));
    start = 0;
    assertEquals(start, scanner.findStartOffset(text, start));
    
    start = TEXT.indexOf("formance");
    int expected = TEXT.indexOf("high-performance");
    assertEquals(expected, scanner.findStartOffset(text, start));
    
    start = TEXT.indexOf("che");
    expected = TEXT.indexOf("Apache");
    assertEquals(expected, scanner.findStartOffset(text, start));
  }

  public void testFindEndOffset() throws Exception {
    StringBuilder text = new StringBuilder(TEXT);
    BoundaryScanner scanner = new SimpleBoundaryScanner();
    
    // test out of range
    int start = TEXT.length() + 1;
    assertEquals(start, scanner.findEndOffset(text, start));
    start = -1;
    assertEquals(start, scanner.findEndOffset(text, start));
    
    start = TEXT.indexOf("full-");
    int expected = TEXT.indexOf("\ntext");
    assertEquals(expected, scanner.findEndOffset(text, start));
  }
}
