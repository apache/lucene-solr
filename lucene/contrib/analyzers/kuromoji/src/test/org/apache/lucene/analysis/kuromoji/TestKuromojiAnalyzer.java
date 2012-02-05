package org.apache.lucene.analysis.kuromoji;

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

import org.apache.lucene.analysis.BaseTokenStreamTestCase;

public class TestKuromojiAnalyzer extends BaseTokenStreamTestCase {
  /** This test fails with NPE when the 
   * stopwords file is missing in classpath */
  public void testResourcesAvailable() {
    new KuromojiAnalyzer(TEST_VERSION_CURRENT);
  }
  
  /**
   * An example sentence, test removal of particles, etc by POS,
   * lemmatization with the basic form, and that position increments
   * and offsets are correct.
   */
  public void testBasics() throws IOException {
    assertAnalyzesTo(new KuromojiAnalyzer(TEST_VERSION_CURRENT), "多くの学生が試験に落ちた。",
        new String[] { "多く", "学生", "試験", "落ちる" },
        new int[] { 0, 3, 6,  9 },
        new int[] { 2, 5, 8, 11 },
        new int[] { 1, 2, 2,  2 }
      );
  }
  
  /**
   * Test that search mode is enabled and working by default
   */
  public void testDecomposition() throws IOException {
    assertAnalyzesTo(new KuromojiAnalyzer(TEST_VERSION_CURRENT), "シニアソフトウェアエンジニア",
        new String[] { "シニア", "ソフトウェア", "エンジニア" }
    );
  }
  
  /**
   * blast random strings against the analyzer
   */
  public void testRandom() throws IOException {
    checkRandomData(random, new KuromojiAnalyzer(TEST_VERSION_CURRENT), atLeast(10000));
  }
}
