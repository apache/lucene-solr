package org.apache.lucene.collation;

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


import com.ibm.icu.text.Collator;

import org.apache.lucene.analysis.Analyzer;

import java.util.Locale;


public class TestICUCollationKeyAnalyzer extends CollationTestBase {

  private Collator collator = Collator.getInstance(new Locale("fa"));
  private Analyzer analyzer = new ICUCollationKeyAnalyzer(collator);

  private String firstRangeBeginning = encodeCollationKey
    (collator.getCollationKey(firstRangeBeginningOriginal).toByteArray());
  private String firstRangeEnd = encodeCollationKey
    (collator.getCollationKey(firstRangeEndOriginal).toByteArray());
  private String secondRangeBeginning = encodeCollationKey
    (collator.getCollationKey(secondRangeBeginningOriginal).toByteArray());
  private String secondRangeEnd = encodeCollationKey
    (collator.getCollationKey(secondRangeEndOriginal).toByteArray());
  
  public void testFarsiRangeFilterCollating() throws Exception {
    testFarsiRangeFilterCollating(analyzer, firstRangeBeginning, firstRangeEnd, 
                                  secondRangeBeginning, secondRangeEnd);
  }
 
  public void testFarsiRangeQueryCollating() throws Exception {
    testFarsiRangeQueryCollating(analyzer, firstRangeBeginning, firstRangeEnd, 
                                 secondRangeBeginning, secondRangeEnd);
  }

  public void testFarsiTermRangeQuery() throws Exception {
    testFarsiTermRangeQuery
      (analyzer, firstRangeBeginning, firstRangeEnd, 
       secondRangeBeginning, secondRangeEnd);
  }

  // Test using various international locales with accented characters (which
  // sort differently depending on locale)
  //
  // Copied (and slightly modified) from 
  // org.apache.lucene.search.TestSort.testInternationalSort()
  //  
  public void testCollationKeySort() throws Exception {
    Analyzer usAnalyzer = new ICUCollationKeyAnalyzer
      (Collator.getInstance(Locale.US));
    Analyzer franceAnalyzer = new ICUCollationKeyAnalyzer
      (Collator.getInstance(Locale.FRANCE));
    Analyzer swedenAnalyzer = new ICUCollationKeyAnalyzer
      (Collator.getInstance(new Locale("sv", "se")));
    Analyzer denmarkAnalyzer = new ICUCollationKeyAnalyzer
      (Collator.getInstance(new Locale("da", "dk")));

    // The ICU Collator and java.text.Collator implementations differ in their
    // orderings - "BFJHD" is the ordering for the ICU Collator for Locale.US.
    testCollationKeySort
    (usAnalyzer, franceAnalyzer, swedenAnalyzer, denmarkAnalyzer, 
     "BFJHD", "ECAGI", "BJDFH", "BJDHF");
  }
  
  public void testThreadSafe() throws Exception {
    int iters = 20 * RANDOM_MULTIPLIER;
    for (int i = 0; i < iters; i++) {
      Locale locale = Locale.GERMAN;
      Collator collator = Collator.getInstance(locale);
      collator.setStrength(Collator.IDENTICAL);
      assertThreadSafe(new ICUCollationKeyAnalyzer(collator));
    }
  }
}
