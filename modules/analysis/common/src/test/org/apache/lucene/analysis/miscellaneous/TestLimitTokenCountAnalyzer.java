package org.apache.lucene.analysis.miscellaneous;

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

import java.io.IOException;
import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

public class TestLimitTokenCountAnalyzer extends BaseTokenStreamTestCase {

  public void testLimitTokenCountAnalyzer() throws IOException {
    Analyzer a = new LimitTokenCountAnalyzer(new WhitespaceAnalyzer(TEST_VERSION_CURRENT), 2);
    // dont use assertAnalyzesTo here, as the end offset is not the end of the string!
    assertTokenStreamContents(a.tokenStream("dummy", new StringReader("1  2     3  4  5")), new String[] { "1", "2" }, new int[] { 0, 3 }, new int[] { 1, 4 }, 4);
    assertTokenStreamContents(a.reusableTokenStream("dummy", new StringReader("1 2 3 4 5")), new String[] { "1", "2" }, new int[] { 0, 2 }, new int[] { 1, 3 }, 3);
    
    a = new LimitTokenCountAnalyzer(new StandardAnalyzer(TEST_VERSION_CURRENT), 2);
    // dont use assertAnalyzesTo here, as the end offset is not the end of the string!
    assertTokenStreamContents(a.tokenStream("dummy", new StringReader("1 2 3 4 5")), new String[] { "1", "2" }, new int[] { 0, 2 }, new int[] { 1, 3 }, 3);
    assertTokenStreamContents(a.reusableTokenStream("dummy", new StringReader("1 2 3 4 5")), new String[] { "1", "2" }, new int[] { 0, 2 }, new int[] { 1, 3 }, 3);
  }

}