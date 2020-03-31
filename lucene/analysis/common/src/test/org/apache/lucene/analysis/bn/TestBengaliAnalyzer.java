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
package org.apache.lucene.analysis.bn;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;

/**
 * Tests the BengaliAnalyzer
 */
public class TestBengaliAnalyzer extends BaseTokenStreamTestCase {

  public void testResourcesAvailable() {
    new BengaliAnalyzer().close();
  }
  
  public void testBasics() throws Exception {
    Analyzer a = new BengaliAnalyzer();

    checkOneTerm(a, "বাড়ী", "বার");
    checkOneTerm(a, "বারী", "বার");
    a.close();
  }
  /**
   * test Digits
   */
  public void testDigits() throws Exception {
    BengaliAnalyzer a = new BengaliAnalyzer();
    checkOneTerm(a, "১২৩৪৫৬৭৮৯০", "1234567890");
    a.close();
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    Analyzer analyzer = new BengaliAnalyzer();
    checkRandomData(random(), analyzer, 200 * RANDOM_MULTIPLIER);
    analyzer.close();
  }
}
