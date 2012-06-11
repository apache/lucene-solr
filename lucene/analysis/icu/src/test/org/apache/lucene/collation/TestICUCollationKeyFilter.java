package org.apache.lucene.collation;

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


import com.ibm.icu.text.Collator;

import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.util.BytesRef;

import java.io.Reader;
import java.util.Locale;

/** @deprecated remove this when ICUCollationKeyFilter is removed */
@Deprecated
public class TestICUCollationKeyFilter extends CollationTestBase {

  private Collator collator = Collator.getInstance(new Locale("fa"));
  private Analyzer analyzer = new TestAnalyzer(collator);

  private BytesRef firstRangeBeginning = new BytesRef(encodeCollationKey
    (collator.getCollationKey(firstRangeBeginningOriginal).toByteArray()));
  private BytesRef firstRangeEnd = new BytesRef(encodeCollationKey
    (collator.getCollationKey(firstRangeEndOriginal).toByteArray()));
  private BytesRef secondRangeBeginning = new BytesRef(encodeCollationKey
    (collator.getCollationKey(secondRangeBeginningOriginal).toByteArray()));
  private BytesRef secondRangeEnd = new BytesRef(encodeCollationKey
    (collator.getCollationKey(secondRangeEndOriginal).toByteArray()));

  
  public final class TestAnalyzer extends Analyzer {
    private Collator _collator;

    TestAnalyzer(Collator collator) {
      _collator = collator;
    }

    @Override
    public TokenStreamComponents createComponents(String fieldName, Reader reader) {
      Tokenizer result = new KeywordTokenizer(reader);
      return new TokenStreamComponents(result, new ICUCollationKeyFilter(result, _collator));
    }
  }

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
    Analyzer usAnalyzer = new TestAnalyzer(Collator.getInstance(Locale.US));
    Analyzer franceAnalyzer 
      = new TestAnalyzer(Collator.getInstance(Locale.FRANCE));
    Analyzer swedenAnalyzer 
      = new TestAnalyzer(Collator.getInstance(new Locale("sv", "se")));
    Analyzer denmarkAnalyzer 
      = new TestAnalyzer(Collator.getInstance(new Locale("da", "dk")));

    // The ICU Collator and java.text.Collator implementations differ in their
    // orderings - "BFJHD" is the ordering for the ICU Collator for Locale.US.
    testCollationKeySort
    (usAnalyzer, franceAnalyzer, swedenAnalyzer, denmarkAnalyzer, 
     "BFJHD", "ECAGI", "BJDFH", "BJDHF");
  }
}
