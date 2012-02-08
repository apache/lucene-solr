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


import org.apache.lucene.analysis.CollationTestBase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.KeywordTokenizer;

import java.text.Collator;
import java.util.Locale;
import java.io.Reader;


public class TestCollationKeyFilter extends CollationTestBase {
  // the sort order of Ø versus U depends on the version of the rules being used
  // for the inherited root locale: Ø's order isnt specified in Locale.US since 
  // its not used in english.
  boolean oStrokeFirst = Collator.getInstance(new Locale("")).compare("Ø", "U") < 0;
  
  // Neither Java 1.4.2 nor 1.5.0 has Farsi Locale collation available in
  // RuleBasedCollator.  However, the Arabic Locale seems to order the Farsi
  // characters properly.
  private Collator collator = Collator.getInstance(new Locale("ar"));
  private Analyzer analyzer = new TestAnalyzer(collator);

  private String firstRangeBeginning = encodeCollationKey
    (collator.getCollationKey(firstRangeBeginningOriginal).toByteArray());
  private String firstRangeEnd = encodeCollationKey
    (collator.getCollationKey(firstRangeEndOriginal).toByteArray());
  private String secondRangeBeginning = encodeCollationKey
    (collator.getCollationKey(secondRangeBeginningOriginal).toByteArray());
  private String secondRangeEnd = encodeCollationKey
    (collator.getCollationKey(secondRangeEndOriginal).toByteArray());

  
  public final class TestAnalyzer extends Analyzer {
    private Collator _collator;

    TestAnalyzer(Collator collator) {
      _collator = collator;
    }

    @Override
    public TokenStream tokenStream(String fieldName, Reader reader) {
      TokenStream result = new KeywordTokenizer(reader);
      result = new CollationKeyFilter(result, _collator);
      return result;
    }
  }

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
  
  public void testCollationKeySort() throws Exception {
    Analyzer usAnalyzer = new TestAnalyzer(Collator.getInstance(Locale.US));
    Analyzer franceAnalyzer 
      = new TestAnalyzer(Collator.getInstance(Locale.FRANCE));
    Analyzer swedenAnalyzer 
      = new TestAnalyzer(Collator.getInstance(new Locale("sv", "se")));
    Analyzer denmarkAnalyzer 
      = new TestAnalyzer(Collator.getInstance(new Locale("da", "dk")));
    
    // The ICU Collator and Sun java.text.Collator implementations differ in their
    // orderings - "BFJDH" is the ordering for java.text.Collator for Locale.US.
    testCollationKeySort
    (usAnalyzer, franceAnalyzer, swedenAnalyzer, denmarkAnalyzer, 
     oStrokeFirst ? "BFJHD" : "BFJDH", "EACGI", "BJDFH", "BJDHF");
  }
}
