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
package org.apache.lucene.analysis.ar;


import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter;

/**
 * Test the Arabic Light Stemmer Filter
 *
 */
public class TestArabicLightStemFilter extends BaseTokenStreamTestCase {
  
  public void testRule1() throws IOException {
    check("حشائش", "حشيشه");
  }    

  public void testRule2() throws IOException {
    check("فوائد", "فائده");
  }       
  
  public void testRule3() throws IOException {
    check("وصايا", "وصيه");
  }
  
  public void testRule4() throws IOException {
    check("دول", "دوله");
  }
  
  public void testRule5() throws IOException {
    check("سطور", "سطر");
  }
      
  public void testRule6() throws IOException {
    check("مدارس", "مدرسه"); 
  }
  
  public void testRule7() throws IOException {
    check("احرف", "حرف");
  } 
  
  public void testRule8() throws IOException {
    check("اجهزه", "جهاز");
  }
  
  public void testRule9() throws IOException {
    check("امراض", "مرض");
  }
  
  public void testRule10() throws IOException {
    check("حدود", "حد");
  }
  
  public void testRule11() throws IOException {
    check("جوانب", "جانب");
  } 
  
  public void testRule12() throws IOException {
    check("اطباء", "طبيب");
  }
  
  public void testRule13() throws IOException {
    check("كلاب", "كلب");
  } 
  
  public void testRule14() throws IOException {
    check("رسام", "راسم");
  } 
  
  public void testRule15() throws IOException {
    check("تقارير", "تقرير");
  }

  public void testRule16() throws IOException {
    check("امم", "امه");
  }
  
  public void testRule17() throws IOException {
    check("يشير", "اشار");
  }
  
  private void check(final String input, final String expected) throws IOException {
    MockTokenizer tokenStream  = whitespaceMockTokenizer(input);
    CharArraySet set = new CharArraySet(17, true);
    set.add("حشيشه");
    set.add("فائده");
    set.add("وصيه");
    set.add("دوله");
    set.add("سطر");
    set.add("مدرسه");
    set.add("حرف");
    set.add("جهاز");
    set.add("مرض");
    set.add("حد");
    set.add("جانب");
    set.add("طبيب");
    set.add("كلب");
    set.add("راسم");
    set.add("تقرير");
    set.add("امه");
    set.add("اشار");
    ArabicLightStemFilter filter = new ArabicLightStemFilter(tokenStream, set);
    filter.setHighAccuracyRequired(true);
    assertTokenStreamContents(filter, new String[]{expected});
  }
  
  public void testEmptyTerm() throws IOException {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new KeywordTokenizer();
        return new TokenStreamComponents(tokenizer, new ArabicLightStemFilter(tokenizer));
      }
    };
    checkOneTerm(a, "", "");
    a.close();
  }
}
