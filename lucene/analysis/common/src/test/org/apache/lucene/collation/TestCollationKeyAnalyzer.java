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
package org.apache.lucene.collation;


import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CollationTestBase;
import org.apache.lucene.util.BytesRef;

import java.text.Collator;
import java.util.Locale;

public class TestCollationKeyAnalyzer extends CollationTestBase { 
  // Neither Java 1.4.2 nor 1.5.0 has Farsi Locale collation available in
  // RuleBasedCollator.  However, the Arabic Locale seems to order the Farsi
  // characters properly.
  private Collator collator = Collator.getInstance(new Locale("ar"));
  private Analyzer analyzer;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    analyzer = new CollationKeyAnalyzer(collator);
  }
  
  @Override
  public void tearDown() throws Exception {
    analyzer.close();
    super.tearDown();
  }

  private BytesRef firstRangeBeginning = new BytesRef(collator.getCollationKey(firstRangeBeginningOriginal).toByteArray());
  private BytesRef firstRangeEnd = new BytesRef(collator.getCollationKey(firstRangeEndOriginal).toByteArray());
  private BytesRef secondRangeBeginning = new BytesRef(collator.getCollationKey(secondRangeBeginningOriginal).toByteArray());
  private BytesRef secondRangeEnd = new BytesRef(collator.getCollationKey(secondRangeEndOriginal).toByteArray());

  public void testFarsiRangeFilterCollating() throws Exception {
    testFarsiRangeFilterCollating
      (analyzer, firstRangeBeginning, firstRangeEnd, 
       secondRangeBeginning, secondRangeEnd);
  }
 
  public void testFarsiRangeQueryCollating() throws Exception {
    testFarsiRangeQueryCollating
      (analyzer, firstRangeBeginning, firstRangeEnd, 
       secondRangeBeginning, secondRangeEnd);
  }

  public void testFarsiTermRangeQuery() throws Exception {
    testFarsiTermRangeQuery
      (analyzer, firstRangeBeginning, firstRangeEnd, 
       secondRangeBeginning, secondRangeEnd);
  }
  
  public void testThreadSafe() throws Exception {
    int iters = 20 * RANDOM_MULTIPLIER;
    for (int i = 0; i < iters; i++) {
      Collator collator = Collator.getInstance(Locale.GERMAN);
      collator.setStrength(Collator.PRIMARY);
      Analyzer analyzer = new CollationKeyAnalyzer(collator);
      assertThreadSafe(analyzer);
      analyzer.close();
    }
  }
}
