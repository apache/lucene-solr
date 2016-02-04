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
package org.apache.lucene.analysis.icu.segmentation;


import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.icu.ICUNormalizer2Filter;
import org.apache.lucene.analysis.icu.tokenattributes.ScriptAttribute;

import com.ibm.icu.lang.UScript;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

public class TestICUTokenizer extends BaseTokenStreamTestCase {
  
  public void testHugeDoc() throws IOException {
    StringBuilder sb = new StringBuilder();
    char whitespace[] = new char[4094];
    Arrays.fill(whitespace, ' ');
    sb.append(whitespace);
    sb.append("testing 1234");
    String input = sb.toString();
    ICUTokenizer tokenizer = new ICUTokenizer(newAttributeFactory(), new DefaultICUTokenizerConfig(false));
    tokenizer.setReader(new StringReader(input));
    assertTokenStreamContents(tokenizer, new String[] { "testing", "1234" });
  }
  
  public void testHugeTerm2() throws IOException {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 40960; i++) {
      sb.append('a');
    }
    String input = sb.toString();
    ICUTokenizer tokenizer = new ICUTokenizer(newAttributeFactory(), new DefaultICUTokenizerConfig(false));
    tokenizer.setReader(new StringReader(input));
    char token[] = new char[4096];
    Arrays.fill(token, 'a');
    String expectedToken = new String(token);
    String expected[] = { 
        expectedToken, expectedToken, expectedToken, 
        expectedToken, expectedToken, expectedToken,
        expectedToken, expectedToken, expectedToken,
        expectedToken
    };
    assertTokenStreamContents(tokenizer, expected);
  }
  
  private Analyzer a; 
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new ICUTokenizer(newAttributeFactory(), new DefaultICUTokenizerConfig(false));
        TokenFilter filter = new ICUNormalizer2Filter(tokenizer);
        return new TokenStreamComponents(tokenizer, filter);
      }
    };
  }
  
  @Override
  public void tearDown() throws Exception {
    a.close();
    super.tearDown();
  }

  public void testArmenian() throws Exception {
    assertAnalyzesTo(a, "Վիքիպեդիայի 13 միլիոն հոդվածները (4,600` հայերեն վիքիպեդիայում) գրվել են կամավորների կողմից ու համարյա բոլոր հոդվածները կարող է խմբագրել ցանկաց մարդ ով կարող է բացել Վիքիպեդիայի կայքը։",
        new String[] { "վիքիպեդիայի", "13", "միլիոն", "հոդվածները", "4,600", "հայերեն", "վիքիպեդիայում", "գրվել", "են", "կամավորների", "կողմից", 
        "ու", "համարյա", "բոլոր", "հոդվածները", "կարող", "է", "խմբագրել", "ցանկաց", "մարդ", "ով", "կարող", "է", "բացել", "վիքիպեդիայի", "կայքը" } );
  }
  
  public void testAmharic() throws Exception {
    assertAnalyzesTo(a, "ዊኪፔድያ የባለ ብዙ ቋንቋ የተሟላ ትክክለኛና ነጻ መዝገበ ዕውቀት (ኢንሳይክሎፒዲያ) ነው። ማንኛውም",
        new String[] { "ዊኪፔድያ", "የባለ", "ብዙ", "ቋንቋ", "የተሟላ", "ትክክለኛና", "ነጻ", "መዝገበ", "ዕውቀት", "ኢንሳይክሎፒዲያ", "ነው", "ማንኛውም" } );
  }
  
  public void testArabic() throws Exception {
    assertAnalyzesTo(a, "الفيلم الوثائقي الأول عن ويكيبيديا يسمى \"الحقيقة بالأرقام: قصة ويكيبيديا\" (بالإنجليزية: Truth in Numbers: The Wikipedia Story)، سيتم إطلاقه في 2008.",
        new String[] { "الفيلم", "الوثائقي", "الأول", "عن", "ويكيبيديا", "يسمى", "الحقيقة", "بالأرقام", "قصة", "ويكيبيديا",
        "بالإنجليزية", "truth", "in", "numbers", "the", "wikipedia", "story", "سيتم", "إطلاقه", "في", "2008" } ); 
  }
  
  public void testAramaic() throws Exception {
    assertAnalyzesTo(a, "ܘܝܩܝܦܕܝܐ (ܐܢܓܠܝܐ: Wikipedia) ܗܘ ܐܝܢܣܩܠܘܦܕܝܐ ܚܐܪܬܐ ܕܐܢܛܪܢܛ ܒܠܫܢ̈ܐ ܣܓܝܐ̈ܐ܂ ܫܡܗ ܐܬܐ ܡܢ ܡ̈ܠܬܐ ܕ\"ܘܝܩܝ\" ܘ\"ܐܝܢܣܩܠܘܦܕܝܐ\"܀",
        new String[] { "ܘܝܩܝܦܕܝܐ", "ܐܢܓܠܝܐ", "wikipedia", "ܗܘ", "ܐܝܢܣܩܠܘܦܕܝܐ", "ܚܐܪܬܐ", "ܕܐܢܛܪܢܛ", "ܒܠܫܢ̈ܐ", "ܣܓܝܐ̈ܐ", "ܫܡܗ",
        "ܐܬܐ", "ܡܢ", "ܡ̈ܠܬܐ", "ܕ", "ܘܝܩܝ", "ܘ", "ܐܝܢܣܩܠܘܦܕܝܐ"});
  }
  
  public void testBengali() throws Exception {
    assertAnalyzesTo(a, "এই বিশ্বকোষ পরিচালনা করে উইকিমিডিয়া ফাউন্ডেশন (একটি অলাভজনক সংস্থা)। উইকিপিডিয়ার শুরু ১৫ জানুয়ারি, ২০০১ সালে। এখন পর্যন্ত ২০০টিরও বেশী ভাষায় উইকিপিডিয়া রয়েছে।",
        new String[] { "এই", "বিশ্বকোষ", "পরিচালনা", "করে", "উইকিমিডিয়া", "ফাউন্ডেশন", "একটি", "অলাভজনক", "সংস্থা", "উইকিপিডিয়ার",
        "শুরু", "১৫", "জানুয়ারি", "২০০১", "সালে", "এখন", "পর্যন্ত", "২০০টিরও", "বেশী", "ভাষায়", "উইকিপিডিয়া", "রয়েছে" });
  }
  
  public void testFarsi() throws Exception {
    assertAnalyzesTo(a, "ویکی پدیای انگلیسی در تاریخ ۲۵ دی ۱۳۷۹ به صورت مکملی برای دانشنامهٔ تخصصی نوپدیا نوشته شد.",
        new String[] { "ویکی", "پدیای", "انگلیسی", "در", "تاریخ", "۲۵", "دی", "۱۳۷۹", "به", "صورت", "مکملی",
        "برای", "دانشنامهٔ", "تخصصی", "نوپدیا", "نوشته", "شد" });
  }
  
  public void testGreek() throws Exception {
    assertAnalyzesTo(a, "Γράφεται σε συνεργασία από εθελοντές με το λογισμικό wiki, κάτι που σημαίνει ότι άρθρα μπορεί να προστεθούν ή να αλλάξουν από τον καθένα.",
        new String[] { "γράφεται", "σε", "συνεργασία", "από", "εθελοντέσ", "με", "το", "λογισμικό", "wiki", "κάτι", "που",
        "σημαίνει", "ότι", "άρθρα", "μπορεί", "να", "προστεθούν", "ή", "να", "αλλάξουν", "από", "τον", "καθένα" });
  }
  
  public void testLao() throws Exception {
    assertAnalyzesTo(a, "ກວ່າດອກ", new String[] { "ກວ່າ", "ດອກ" });
    assertAnalyzesTo(a, "ພາສາລາວ", new String[] { "ພາສາ", "ລາວ"}, new String[] { "<ALPHANUM>", "<ALPHANUM>" });
  }
  
  public void testMyanmar() throws Exception {
    assertAnalyzesTo(a, "သက်ဝင်လှုပ်ရှားစေပြီး", new String[] { "သက်ဝင်", "လှုပ်ရှား", "စေ", "ပြီး" });
  }
  
  public void testThai() throws Exception {
    assertAnalyzesTo(a, "การที่ได้ต้องแสดงว่างานดี. แล้วเธอจะไปไหน? ๑๒๓๔",
        new String[] { "การ", "ที่", "ได้", "ต้อง", "แสดง", "ว่า", "งาน", "ดี", "แล้ว", "เธอ", "จะ", "ไป", "ไหน", "๑๒๓๔"});
  }
  
  public void testTibetan() throws Exception {
    assertAnalyzesTo(a, "སྣོན་མཛོད་དང་ལས་འདིས་བོད་ཡིག་མི་ཉམས་གོང་འཕེལ་དུ་གཏོང་བར་ཧ་ཅང་དགེ་མཚན་མཆིས་སོ། །",
        new String[] { "སྣོན", "མཛོད", "དང", "ལས", "འདིས", "བོད", "ཡིག", "མི", "ཉམས", "གོང", "འཕེལ", "དུ", "གཏོང", "བར", "ཧ", "ཅང", "དགེ", "མཚན", "མཆིས", "སོ" });
  }
  
  /*
   * For chinese, tokenize as char (these can later form bigrams or whatever)
   */
  public void testChinese() throws Exception {
    assertAnalyzesTo(a, "我是中国人。 １２３４ Ｔｅｓｔｓ ",
        new String[] { "我", "是", "中", "国", "人", "1234", "tests"});
  }
  
  public void testHebrew() throws Exception {
    assertAnalyzesTo(a, "דנקנר תקף את הדו\"ח",
        new String[] { "דנקנר", "תקף", "את", "הדו\"ח" });
    assertAnalyzesTo(a, "חברת בת של מודי'ס",
        new String[] { "חברת", "בת", "של", "מודי'ס" });
  }
  
  public void testEmpty() throws Exception {
    assertAnalyzesTo(a, "", new String[] {});
    assertAnalyzesTo(a, ".", new String[] {});
    assertAnalyzesTo(a, " ", new String[] {});
  }
  
  /* test various jira issues this analyzer is related to */
  
  public void testLUCENE1545() throws Exception {
    /*
     * Standard analyzer does not correctly tokenize combining character U+0364 COMBINING LATIN SMALL LETTRE E.
     * The word "moͤchte" is incorrectly tokenized into "mo" "chte", the combining character is lost.
     * Expected result is only on token "moͤchte".
     */
    assertAnalyzesTo(a, "moͤchte", new String[] { "moͤchte" }); 
  }
  
  /* Tests from StandardAnalyzer, just to show behavior is similar */
  public void testAlphanumericSA() throws Exception {
    // alphanumeric tokens
    assertAnalyzesTo(a, "B2B", new String[]{"b2b"});
    assertAnalyzesTo(a, "2B", new String[]{"2b"});
  }

  public void testDelimitersSA() throws Exception {
    // other delimiters: "-", "/", ","
    assertAnalyzesTo(a, "some-dashed-phrase", new String[]{"some", "dashed", "phrase"});
    assertAnalyzesTo(a, "dogs,chase,cats", new String[]{"dogs", "chase", "cats"});
    assertAnalyzesTo(a, "ac/dc", new String[]{"ac", "dc"});
  }

  public void testApostrophesSA() throws Exception {
    // internal apostrophes: O'Reilly, you're, O'Reilly's
    assertAnalyzesTo(a, "O'Reilly", new String[]{"o'reilly"});
    assertAnalyzesTo(a, "you're", new String[]{"you're"});
    assertAnalyzesTo(a, "she's", new String[]{"she's"});
    assertAnalyzesTo(a, "Jim's", new String[]{"jim's"});
    assertAnalyzesTo(a, "don't", new String[]{"don't"});
    assertAnalyzesTo(a, "O'Reilly's", new String[]{"o'reilly's"});
  }

  public void testNumericSA() throws Exception {
    // floating point, serial, model numbers, ip addresses, etc.
    // every other segment must have at least one digit
    assertAnalyzesTo(a, "21.35", new String[]{"21.35"});
    assertAnalyzesTo(a, "R2D2 C3PO", new String[]{"r2d2", "c3po"});
    assertAnalyzesTo(a, "216.239.63.104", new String[]{"216.239.63.104"});
    assertAnalyzesTo(a, "216.239.63.104", new String[]{"216.239.63.104"});
  }

  public void testTextWithNumbersSA() throws Exception {
    // numbers
    assertAnalyzesTo(a, "David has 5000 bones", new String[]{"david", "has", "5000", "bones"});
  }

  public void testVariousTextSA() throws Exception {
    // various
    assertAnalyzesTo(a, "C embedded developers wanted", new String[]{"c", "embedded", "developers", "wanted"});
    assertAnalyzesTo(a, "foo bar FOO BAR", new String[]{"foo", "bar", "foo", "bar"});
    assertAnalyzesTo(a, "foo      bar .  FOO <> BAR", new String[]{"foo", "bar", "foo", "bar"});
    assertAnalyzesTo(a, "\"QUOTED\" word", new String[]{"quoted", "word"});
  }

  public void testKoreanSA() throws Exception {
    // Korean words
    assertAnalyzesTo(a, "안녕하세요 한글입니다", new String[]{"안녕하세요", "한글입니다"});
  }
  
  public void testReusableTokenStream() throws Exception {
    assertAnalyzesTo(a, "སྣོན་མཛོད་དང་ལས་འདིས་བོད་ཡིག་མི་ཉམས་གོང་འཕེལ་དུ་གཏོང་བར་ཧ་ཅང་དགེ་མཚན་མཆིས་སོ། །",
        new String[] { "སྣོན", "མཛོད", "དང", "ལས", "འདིས", "བོད", "ཡིག", "མི", "ཉམས", "གོང", 
                      "འཕེལ", "དུ", "གཏོང", "བར", "ཧ", "ཅང", "དགེ", "མཚན", "མཆིས", "སོ" });
  }
  
  public void testOffsets() throws Exception {
    assertAnalyzesTo(a, "David has 5000 bones", 
        new String[] {"david", "has", "5000", "bones"},
        new int[] {0, 6, 10, 15},
        new int[] {5, 9, 14, 20});
  }
  
  public void testTypes() throws Exception {
    assertAnalyzesTo(a, "David has 5000 bones", 
        new String[] {"david", "has", "5000", "bones"},
        new String[] { "<ALPHANUM>", "<ALPHANUM>", "<NUM>", "<ALPHANUM>" });
  }
  
  public void testKorean() throws Exception {
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "훈민정음",
        new String[] { "훈민정음" },
        new String[] { "<HANGUL>" });
  }
  
  public void testJapanese() throws Exception {
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "仮名遣い カタカナ",
        new String[] { "仮", "名", "遣", "い", "カタカナ" },
        new String[] { "<IDEOGRAPHIC>", "<IDEOGRAPHIC>", "<IDEOGRAPHIC>", "<HIRAGANA>", "<KATAKANA>" });
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    checkRandomData(random(), a, 1000*RANDOM_MULTIPLIER);
  }
  
  /** blast some random large strings through the analyzer */
  public void testRandomHugeStrings() throws Exception {
    Random random = random();
    checkRandomData(random, a, 100*RANDOM_MULTIPLIER, 8192);
  }
  
  public void testTokenAttributes() throws Exception {
    try (TokenStream ts = a.tokenStream("dummy", "This is a test")) {
      ScriptAttribute scriptAtt = ts.addAttribute(ScriptAttribute.class);
      ts.reset();
      while (ts.incrementToken()) {
        assertEquals(UScript.LATIN, scriptAtt.getCode());
        assertEquals(UScript.getName(UScript.LATIN), scriptAtt.getName());
        assertEquals(UScript.getShortName(UScript.LATIN), scriptAtt.getShortName());
        assertTrue(ts.reflectAsString(false).contains("script=Latin"));
      }
      ts.end();
    }
  }
  
  /** test for bugs like http://bugs.icu-project.org/trac/ticket/10767 */
  public void testICUConcurrency() throws Exception {
    int numThreads = 8;
    final CountDownLatch startingGun = new CountDownLatch(1);
    Thread threads[] = new Thread[numThreads];
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new Thread() {
        @Override
        public void run() {
          try {
            startingGun.await();
            long tokenCount = 0;
            final String contents = "英 เบียร์ ビール ເບຍ abc";
            for (int i = 0; i < 1000; i++) {
              try (Tokenizer tokenizer = new ICUTokenizer()) {
                tokenizer.setReader(new StringReader(contents));
                tokenizer.reset();
                while (tokenizer.incrementToken()) {
                  tokenCount++;
                }
                tokenizer.end();
              }
            }
            if (VERBOSE) {
              System.out.println(tokenCount);
            }
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        } 
      };
      threads[i].start();
    }
    startingGun.countDown();
    for (int i = 0; i < threads.length; i++) {
      threads[i].join();
    }
  }
}
