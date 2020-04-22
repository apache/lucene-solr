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
package org.apache.lucene.analysis.standard;


import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockGraphTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.TestUtil;

public class TestStandardAnalyzer extends BaseTokenStreamTestCase {

  // LUCENE-5897: slow tokenization of strings of the form (\p{WB:ExtendNumLet}[\p{WB:Format}\p{WB:Extend}]*)+
  @Slow
  public void testLargePartiallyMatchingToken() throws Exception {
    // TODO: get these lists of chars matching a property from ICU4J
    // http://www.unicode.org/Public/6.3.0/ucd/auxiliary/WordBreakProperty.txt
    char[] WordBreak_ExtendNumLet_chars = "_\u203f\u2040\u2054\ufe33\ufe34\ufe4d\ufe4e\ufe4f\uff3f".toCharArray();

    // http://www.unicode.org/Public/6.3.0/ucd/auxiliary/WordBreakProperty.txt
    int[] WordBreak_Format_chars // only the first char in ranges 
        = { 0xAD, 0x600, 0x61C, 0x6DD, 0x70F, 0x180E, 0x200E, 0x202A, 0x2060, 0x2066, 0xFEFF,
            0xFFF9, 0x110BD, 0x1D173, 0xE0001, 0xE0020 };

    // http://www.unicode.org/Public/6.3.0/ucd/auxiliary/WordBreakProperty.txt
    int[] WordBreak_Extend_chars // only the first char in ranges
        = { 0x300, 0x483, 0x591, 0x5bf, 0x5c1, 0x5c4, 0x5c7, 0x610, 0x64b, 0x670, 0x6d6, 0x6df,
            0x6e7, 0x6ea, 0x711, 0x730, 0x7a6, 0x7eb, 0x816, 0x81b, 0x825, 0x829, 0x859, 0x8e4,
            0x900, 0x93a, 0x93e, 0x951, 0x962, 0x981, 0x9bc, 0x9be, 0x9c7, 0x9cb, 0x9d7, 0x9e2,
            0xa01, 0xa3c, 0xa3e, 0xa47, 0xa4b, 0xa51, 0xa70, 0xa75, 0xa81, 0xabc, 0xabe, 0xac7,
            0xacb, 0xae2, 0xb01, 0xb3c, 0xb3e, 0xb47, 0xb4b, 0xb56, 0xb62, 0xb82, 0xbbe, 0xbc6,
            0xbca, 0xbd7, 0xc01, 0xc3e, 0xc46, 0xc4a, 0xc55, 0xc62, 0xc82, 0xcbc, 0xcbe, 0xcc6,
            0xcca, 0xcd5, 0xce2, 0xd02, 0xd3e, 0xd46, 0xd4a, 0xd57, 0xd62, 0xd82, 0xdca, 0xdcf,
            0xdd6, 0xdd8, 0xdf2, 0xe31, 0xe34, 0xe47, 0xeb1, 0xeb4, 0xebb, 0xec8, 0xf18, 0xf35,
            0xf37, 0xf39, 0xf3e, 0xf71, 0xf86, 0xf8d, 0xf99, 0xfc6, 0x102b, 0x1056, 0x105e, 0x1062,
            0x1067, 0x1071, 0x1082, 0x108f, 0x109a, 0x135d, 0x1712, 0x1732, 0x1752, 0x1772, 0x17b4, 
            0x17dd, 0x180b, 0x18a9, 0x1920, 0x1930, 0x19b0, 0x19c8, 0x1a17, 0x1a55, 0x1a60, 0x1a7f,
            0x1b00, 0x1b34, 0x1b6b, 0x1b80, 0x1ba1, 0x1be6, 0x1c24, 0x1cd0, 0x1cd4, 0x1ced, 0x1cf2, 
            0x1dc0, 0x1dfc, 0x200c, 0x20d0, 0x2cef, 0x2d7f, 0x2de0, 0x302a, 0x3099, 0xa66f, 0xa674,
            0xa69f, 0xa6f0, 0xa802, 0xa806, 0xa80b, 0xa823, 0xa880, 0xa8b4, 0xa8e0, 0xa926, 0xa947, 
            0xa980, 0xa9b3, 0xaa29, 0xaa43, 0xaa4c, 0xaa7b, 0xaab0, 0xaab2, 0xaab7, 0xaabe, 0xaac1,
            0xaaeb, 0xaaf5, 0xabe3, 0xabec, 0xfb1e, 0xfe00, 0xfe20, 0xff9e, 0x101fd, 0x10a01,
            0x10a05, 0x10a0C, 0x10a38, 0x10a3F, 0x11000, 0x11001, 0x11038, 0x11080, 0x11082,
            0x110b0, 0x110b3, 0x110b7, 0x110b9, 0x11100, 0x11127, 0x1112c, 0x11180, 0x11182,
            0x111b3, 0x111b6, 0x111bF, 0x116ab, 0x116ac, 0x116b0, 0x116b6, 0x16f51, 0x16f8f,
            0x1d165, 0x1d167, 0x1d16d, 0x1d17b, 0x1d185, 0x1d1aa, 0x1d242, 0xe0100 }; 
        
    StringBuilder builder = new StringBuilder();
    int numChars = TestUtil.nextInt(random(), 100 * 1024, 1024 * 1024);
    for (int i = 0 ; i < numChars ; ) {
      builder.append(WordBreak_ExtendNumLet_chars[random().nextInt(WordBreak_ExtendNumLet_chars.length)]);
      ++i;
      if (random().nextBoolean()) {
        int numFormatExtendChars = TestUtil.nextInt(random(), 1, 8);
        for (int j = 0; j < numFormatExtendChars; ++j) {
          int codepoint;
          if (random().nextBoolean()) {
            codepoint = WordBreak_Format_chars[random().nextInt(WordBreak_Format_chars.length)];
          } else {
            codepoint = WordBreak_Extend_chars[random().nextInt(WordBreak_Extend_chars.length)];
          }
          char[] chars = Character.toChars(codepoint);
          builder.append(chars);
          i += chars.length;
        }
      }
    }
    StandardTokenizer ts = new StandardTokenizer();
    ts.setReader(new StringReader(builder.toString()));
    ts.reset();
    while (ts.incrementToken()) { }
    ts.end();
    ts.close();

    int newBufferSize = TestUtil.nextInt(random(), 200, 8192);
    ts.setMaxTokenLength(newBufferSize); // try a different buffer size
    ts.setReader(new StringReader(builder.toString()));
    ts.reset();
    while (ts.incrementToken()) { }
    ts.end();
    ts.close();
  }
  
  public void testHugeDoc() throws IOException {
    StringBuilder sb = new StringBuilder();
    char whitespace[] = new char[4094];
    Arrays.fill(whitespace, ' ');
    sb.append(whitespace);
    sb.append("testing 1234");
    String input = sb.toString();
    StandardTokenizer tokenizer = new StandardTokenizer();
    tokenizer.setReader(new StringReader(input));
    BaseTokenStreamTestCase.assertTokenStreamContents(tokenizer, new String[] { "testing", "1234" });
  }

  private Analyzer a;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new StandardTokenizer(newAttributeFactory());
        return new TokenStreamComponents(tokenizer);
      }
    };
  }
  
  @Override
  public void tearDown() throws Exception {
    a.close();
    super.tearDown();
  }

  public void testArmenian() throws Exception {
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "Վիքիպեդիայի 13 միլիոն հոդվածները (4,600` հայերեն վիքիպեդիայում) գրվել են կամավորների կողմից ու համարյա բոլոր հոդվածները կարող է խմբագրել ցանկաց մարդ ով կարող է բացել Վիքիպեդիայի կայքը։",
        new String[] { "Վիքիպեդիայի", "13", "միլիոն", "հոդվածները", "4,600", "հայերեն", "վիքիպեդիայում", "գրվել", "են", "կամավորների", "կողմից", 
        "ու", "համարյա", "բոլոր", "հոդվածները", "կարող", "է", "խմբագրել", "ցանկաց", "մարդ", "ով", "կարող", "է", "բացել", "Վիքիպեդիայի", "կայքը" } );
  }
  
  public void testAmharic() throws Exception {
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "ዊኪፔድያ የባለ ብዙ ቋንቋ የተሟላ ትክክለኛና ነጻ መዝገበ ዕውቀት (ኢንሳይክሎፒዲያ) ነው። ማንኛውም",
        new String[] { "ዊኪፔድያ", "የባለ", "ብዙ", "ቋንቋ", "የተሟላ", "ትክክለኛና", "ነጻ", "መዝገበ", "ዕውቀት", "ኢንሳይክሎፒዲያ", "ነው", "ማንኛውም" } );
  }
  
  public void testArabic() throws Exception {
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "الفيلم الوثائقي الأول عن ويكيبيديا يسمى \"الحقيقة بالأرقام: قصة ويكيبيديا\" (بالإنجليزية: Truth in Numbers: The Wikipedia Story)، سيتم إطلاقه في 2008.",
        new String[] { "الفيلم", "الوثائقي", "الأول", "عن", "ويكيبيديا", "يسمى", "الحقيقة", "بالأرقام", "قصة", "ويكيبيديا",
        "بالإنجليزية", "Truth", "in", "Numbers", "The", "Wikipedia", "Story", "سيتم", "إطلاقه", "في", "2008" } ); 
  }
  
  public void testAramaic() throws Exception {
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "ܘܝܩܝܦܕܝܐ (ܐܢܓܠܝܐ: Wikipedia) ܗܘ ܐܝܢܣܩܠܘܦܕܝܐ ܚܐܪܬܐ ܕܐܢܛܪܢܛ ܒܠܫܢ̈ܐ ܣܓܝܐ̈ܐ܂ ܫܡܗ ܐܬܐ ܡܢ ܡ̈ܠܬܐ ܕ\"ܘܝܩܝ\" ܘ\"ܐܝܢܣܩܠܘܦܕܝܐ\"܀",
        new String[] { "ܘܝܩܝܦܕܝܐ", "ܐܢܓܠܝܐ", "Wikipedia", "ܗܘ", "ܐܝܢܣܩܠܘܦܕܝܐ", "ܚܐܪܬܐ", "ܕܐܢܛܪܢܛ", "ܒܠܫܢ̈ܐ", "ܣܓܝܐ̈ܐ", "ܫܡܗ",
        "ܐܬܐ", "ܡܢ", "ܡ̈ܠܬܐ", "ܕ", "ܘܝܩܝ", "ܘ", "ܐܝܢܣܩܠܘܦܕܝܐ"});
  }
  
  public void testBengali() throws Exception {
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "এই বিশ্বকোষ পরিচালনা করে উইকিমিডিয়া ফাউন্ডেশন (একটি অলাভজনক সংস্থা)। উইকিপিডিয়ার শুরু ১৫ জানুয়ারি, ২০০১ সালে। এখন পর্যন্ত ২০০টিরও বেশী ভাষায় উইকিপিডিয়া রয়েছে।",
        new String[] { "এই", "বিশ্বকোষ", "পরিচালনা", "করে", "উইকিমিডিয়া", "ফাউন্ডেশন", "একটি", "অলাভজনক", "সংস্থা", "উইকিপিডিয়ার",
        "শুরু", "১৫", "জানুয়ারি", "২০০১", "সালে", "এখন", "পর্যন্ত", "২০০টিরও", "বেশী", "ভাষায়", "উইকিপিডিয়া", "রয়েছে" });
  }
  
  public void testFarsi() throws Exception {
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "ویکی پدیای انگلیسی در تاریخ ۲۵ دی ۱۳۷۹ به صورت مکملی برای دانشنامهٔ تخصصی نوپدیا نوشته شد.",
        new String[] { "ویکی", "پدیای", "انگلیسی", "در", "تاریخ", "۲۵", "دی", "۱۳۷۹", "به", "صورت", "مکملی",
        "برای", "دانشنامهٔ", "تخصصی", "نوپدیا", "نوشته", "شد" });
  }
  
  public void testGreek() throws Exception {
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "Γράφεται σε συνεργασία από εθελοντές με το λογισμικό wiki, κάτι που σημαίνει ότι άρθρα μπορεί να προστεθούν ή να αλλάξουν από τον καθένα.",
        new String[] { "Γράφεται", "σε", "συνεργασία", "από", "εθελοντές", "με", "το", "λογισμικό", "wiki", "κάτι", "που",
        "σημαίνει", "ότι", "άρθρα", "μπορεί", "να", "προστεθούν", "ή", "να", "αλλάξουν", "από", "τον", "καθένα" });
  }

  public void testThai() throws Exception {
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "การที่ได้ต้องแสดงว่างานดี. แล้วเธอจะไปไหน? ๑๒๓๔",
        new String[] { "การที่ได้ต้องแสดงว่างานดี", "แล้วเธอจะไปไหน", "๑๒๓๔" });
  }
  
  public void testLao() throws Exception {
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "ສາທາລະນະລັດ ປະຊາທິປະໄຕ ປະຊາຊົນລາວ", 
        new String[] { "ສາທາລະນະລັດ", "ປະຊາທິປະໄຕ", "ປະຊາຊົນລາວ" });
  }
  
  public void testTibetan() throws Exception {
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "སྣོན་མཛོད་དང་ལས་འདིས་བོད་ཡིག་མི་ཉམས་གོང་འཕེལ་དུ་གཏོང་བར་ཧ་ཅང་དགེ་མཚན་མཆིས་སོ། །",
                     new String[] { "སྣོན", "མཛོད", "དང", "ལས", "འདིས", "བོད", "ཡིག", 
                                    "མི", "ཉམས", "གོང", "འཕེལ", "དུ", "གཏོང", "བར", 
                                    "ཧ", "ཅང", "དགེ", "མཚན", "མཆིས", "སོ" });
  }
  
  /*
   * For chinese, tokenize as char (these can later form bigrams or whatever)
   */
  public void testChinese() throws Exception {
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "我是中国人。 １２３４ Ｔｅｓｔｓ ",
        new String[] { "我", "是", "中", "国", "人", "１２３４", "Ｔｅｓｔｓ"});
  }
  
  public void testEmpty() throws Exception {
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "", new String[] {});
    BaseTokenStreamTestCase.assertAnalyzesTo(a, ".", new String[] {});
    BaseTokenStreamTestCase.assertAnalyzesTo(a, " ", new String[] {});
  }
  
  /* test various jira issues this analyzer is related to */
  
  public void testLUCENE1545() throws Exception {
    /*
     * Standard analyzer does not correctly tokenize combining character U+0364 COMBINING LATIN SMALL LETTRE E.
     * The word "moͤchte" is incorrectly tokenized into "mo" "chte", the combining character is lost.
     * Expected result is only on token "moͤchte".
     */
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "moͤchte", new String[] { "moͤchte" }); 
  }
  
  /* Tests from StandardAnalyzer, just to show behavior is similar */
  public void testAlphanumericSA() throws Exception {
    // alphanumeric tokens
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "B2B", new String[]{"B2B"});
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "2B", new String[]{"2B"});
  }

  public void testDelimitersSA() throws Exception {
    // other delimiters: "-", "/", ","
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "some-dashed-phrase", new String[]{"some", "dashed", "phrase"});
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "dogs,chase,cats", new String[]{"dogs", "chase", "cats"});
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "ac/dc", new String[]{"ac", "dc"});
  }

  public void testApostrophesSA() throws Exception {
    // internal apostrophes: O'Reilly, you're, O'Reilly's
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "O'Reilly", new String[]{"O'Reilly"});
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "you're", new String[]{"you're"});
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "she's", new String[]{"she's"});
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "Jim's", new String[]{"Jim's"});
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "don't", new String[]{"don't"});
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "O'Reilly's", new String[]{"O'Reilly's"});
  }

  public void testNumericSA() throws Exception {
    // floating point, serial, model numbers, ip addresses, etc.
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "21.35", new String[]{"21.35"});
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "R2D2 C3PO", new String[]{"R2D2", "C3PO"});
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "216.239.63.104", new String[]{"216.239.63.104"});
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "216.239.63.104", new String[]{"216.239.63.104"});
  }

  public void testTextWithNumbersSA() throws Exception {
    // numbers
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "David has 5000 bones", new String[]{"David", "has", "5000", "bones"});
  }

  public void testVariousTextSA() throws Exception {
    // various
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "C embedded developers wanted", new String[]{"C", "embedded", "developers", "wanted"});
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "foo bar FOO BAR", new String[]{"foo", "bar", "FOO", "BAR"});
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "foo      bar .  FOO <> BAR", new String[]{"foo", "bar", "FOO", "BAR"});
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "\"QUOTED\" word", new String[]{"QUOTED", "word"});
  }

  public void testKoreanSA() throws Exception {
    // Korean words
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "안녕하세요 한글입니다", new String[]{"안녕하세요", "한글입니다"});
  }
  
  public void testOffsets() throws Exception {
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "David has 5000 bones", 
        new String[] {"David", "has", "5000", "bones"},
        new int[] {0, 6, 10, 15},
        new int[] {5, 9, 14, 20});
  }
  
  public void testTypes() throws Exception {
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "David has 5000 bones", 
        new String[] {"David", "has", "5000", "bones"},
        new String[] { "<ALPHANUM>", "<ALPHANUM>", "<NUM>", "<ALPHANUM>" });
  }
  
  public void testUnicodeWordBreaks() throws Exception {
    WordBreakTestUnicode_9_0_0 wordBreakTest = new WordBreakTestUnicode_9_0_0();
    wordBreakTest.test(a);
  }
  
  public void testSupplementary() throws Exception {
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "𩬅艱鍟䇹愯瀛", 
        new String[] {"𩬅", "艱", "鍟", "䇹", "愯", "瀛"},
        new String[] { "<IDEOGRAPHIC>", "<IDEOGRAPHIC>", "<IDEOGRAPHIC>", "<IDEOGRAPHIC>", "<IDEOGRAPHIC>", "<IDEOGRAPHIC>" });
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
  
  public void testCombiningMarks() throws Exception {
    checkOneTerm(a, "ざ", "ざ"); // hiragana
    checkOneTerm(a, "ザ", "ザ"); // katakana
    checkOneTerm(a, "壹゙", "壹゙"); // ideographic
    checkOneTerm(a, "아゙",  "아゙"); // hangul
  }

  /**
   * Multiple consecutive chars in \p{WB:MidLetter}, \p{WB:MidNumLet},
   * and/or \p{MidNum} should trigger a token split.
   */
  public void testMid() throws Exception {
    // ':' is in \p{WB:MidLetter}, which should trigger a split unless there is a Letter char on both sides
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "A:B", new String[] { "A:B" });
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "A::B", new String[] { "A", "B" });

    // '.' is in \p{WB:MidNumLet}, which should trigger a split unless there is a Letter or Numeric char on both sides
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "1.2", new String[] { "1.2" });
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "A.B", new String[] { "A.B" });
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "1..2", new String[] { "1", "2" });
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "A..B", new String[] { "A", "B" });

    // ',' is in \p{WB:MidNum}, which should trigger a split unless there is a Numeric char on both sides
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "1,2", new String[] { "1,2" });
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "1,,2", new String[] { "1", "2" });

    // Mixed consecutive \p{WB:MidLetter} and \p{WB:MidNumLet} should trigger a split
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "A.:B", new String[] { "A", "B" });
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "A:.B", new String[] { "A", "B" });

    // Mixed consecutive \p{WB:MidNum} and \p{WB:MidNumLet} should trigger a split
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "1,.2", new String[] { "1", "2" });
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "1.,2", new String[] { "1", "2" });

    // '_' is in \p{WB:ExtendNumLet}

    BaseTokenStreamTestCase.assertAnalyzesTo(a, "A:B_A:B", new String[] { "A:B_A:B" });
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "A:B_A::B", new String[] { "A:B_A", "B" });

    BaseTokenStreamTestCase.assertAnalyzesTo(a, "1.2_1.2", new String[] { "1.2_1.2" });
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "A.B_A.B", new String[] { "A.B_A.B" });
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "1.2_1..2", new String[] { "1.2_1", "2" });
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "A.B_A..B", new String[] { "A.B_A", "B" });

    BaseTokenStreamTestCase.assertAnalyzesTo(a, "1,2_1,2", new String[] { "1,2_1,2" });
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "1,2_1,,2", new String[] { "1,2_1", "2" });

    BaseTokenStreamTestCase.assertAnalyzesTo(a, "C_A.:B", new String[] { "C_A", "B" });
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "C_A:.B", new String[] { "C_A", "B" });

    BaseTokenStreamTestCase.assertAnalyzesTo(a, "3_1,.2", new String[] { "3_1", "2" });
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "3_1.,2", new String[] { "3_1", "2" });
  }

  /** simple emoji */
  public void testEmoji() throws Exception {
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "💩 💩💩",
        new String[] { "💩", "💩", "💩" },
        new String[] { "<EMOJI>", "<EMOJI>", "<EMOJI>" });
  }

  /** emoji zwj sequence */
  public void testEmojiSequence() throws Exception {
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "👩‍❤️‍👩",
        new String[] { "👩‍❤️‍👩" },
        new String[] { "<EMOJI>" });
  }

  /** emoji zwj sequence with fitzpatrick modifier */
  public void testEmojiSequenceWithModifier() throws Exception {
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "👨🏼‍⚕️",
        new String[] { "👨🏼‍⚕️" },
        new String[] { "<EMOJI>" });
  }

  /** regional indicator */
  public void testEmojiRegionalIndicator() throws Exception {
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "🇺🇸🇺🇸",
        new String[] { "🇺🇸", "🇺🇸" },
        new String[] { "<EMOJI>", "<EMOJI>" });
  }

  /** variation sequence */
  public void testEmojiVariationSequence() throws Exception {
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "#️⃣",
        new String[] { "#️⃣" },
        new String[] { "<EMOJI>" });
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "3️⃣",
        new String[] { "3️⃣",},
        new String[] { "<EMOJI>" });

    // text presentation sequences
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "#\uFE0E",
        new String[] { },
        new String[] { });
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "3\uFE0E",  // \uFE0E is included in \p{WB:Extend}
        new String[] { "3\uFE0E",},
        new String[] { "<NUM>" });
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "\u2B55\uFE0E",     // \u2B55 = HEAVY BLACK CIRCLE
        new String[] { "\u2B55",},
        new String[] { "<EMOJI>" });
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "\u2B55\uFE0E\u200D\u2B55\uFE0E",
        new String[] { "\u2B55", "\u200D\u2B55"},
        new String[] { "<EMOJI>", "<EMOJI>" });
  }

  public void testEmojiTagSequence() throws Exception {
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "🏴󠁧󠁢󠁥󠁮󠁧󠁿",
        new String[] { "🏴󠁧󠁢󠁥󠁮󠁧󠁿" },
        new String[] { "<EMOJI>" });
  }

  public void testEmojiTokenization() throws Exception {
    // simple emoji around latin
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "poo💩poo",
        new String[] { "poo", "💩", "poo" },
        new String[] { "<ALPHANUM>", "<EMOJI>", "<ALPHANUM>" });
    // simple emoji around non-latin
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "💩中國💩",
        new String[] { "💩", "中", "國", "💩" },
        new String[] { "<EMOJI>", "<IDEOGRAPHIC>", "<IDEOGRAPHIC>", "<EMOJI>" });
  }
  
  public void testUnicodeEmojiTests() throws Exception {
    EmojiTokenizationTestUnicode_11_0 emojiTest = new EmojiTokenizationTestUnicode_11_0();
    emojiTest.test(a);
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    Analyzer analyzer = new StandardAnalyzer();
    checkRandomData(random(), analyzer, 200 * RANDOM_MULTIPLIER);
    analyzer.close();
  }
  
  /** blast some random large strings through the analyzer */
  public void testRandomHugeStrings() throws Exception {
    Analyzer analyzer = new StandardAnalyzer();
    checkRandomData(random(), analyzer, 20*RANDOM_MULTIPLIER, 8192);
    analyzer.close();
  }

  // Adds random graph after:
  public void testRandomHugeStringsGraphAfter() throws Exception {
    Random random = random();
    Analyzer analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new StandardTokenizer(newAttributeFactory());
        TokenStream tokenStream = new MockGraphTokenFilter(random(), tokenizer);
        return new TokenStreamComponents(tokenizer, tokenStream);
      }
    };
    checkRandomData(random, analyzer, 20*RANDOM_MULTIPLIER, 8192);
    analyzer.close();
  }

  public void testNormalize() {
    Analyzer a = new StandardAnalyzer();
    assertEquals(new BytesRef("\"\\à3[]()! cz@"), a.normalize("dummy", "\"\\À3[]()! Cz@"));
  }

  public void testMaxTokenLengthDefault() throws Exception {
    StandardAnalyzer a = new StandardAnalyzer();

    StringBuilder bToken = new StringBuilder();
    // exact max length:
    for(int i=0;i<StandardAnalyzer.DEFAULT_MAX_TOKEN_LENGTH;i++) {
      bToken.append('b');
    }

    String bString = bToken.toString();
    // first bString is exact max default length; next one is 1 too long
    String input = "x " + bString + " " + bString + "b";
    assertAnalyzesTo(a, input.toString(), new String[] {"x", bString, bString, "b"});
    a.close();
  }

  public void testMaxTokenLengthNonDefault() throws Exception {
    StandardAnalyzer a = new StandardAnalyzer();
    a.setMaxTokenLength(5);
    assertAnalyzesTo(a, "ab cd toolong xy z", new String[]{"ab", "cd", "toolo", "ng", "xy", "z"});
    a.close();
  }

  public void testSplitSurrogatePairWithSpoonFeedReader() throws Exception {
    String text = "12345678\ud800\udf00"; // U+D800 U+DF00 = U+10300 = 𐌀 (OLD ITALIC LETTER A)
    
    // Collect tokens with normal reader
    StandardAnalyzer a = new StandardAnalyzer();
    TokenStream ts = a.tokenStream("dummy", text);
    List<String> tokens = new ArrayList<>();
    CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
    ts.reset();
    while (ts.incrementToken()) {
      tokens.add(termAtt.toString());
    }
    ts.end();
    ts.close();

    // Tokens from a spoon-feed reader should be the same as from a normal reader
    // The 9th char is a high surrogate, so the 9-max-chars spoon-feed reader will split the surrogate pair at a read boundary
    Reader reader = new SpoonFeedMaxCharsReaderWrapper(9, new StringReader(text));
    ts = a.tokenStream("dummy", reader);
    termAtt = ts.addAttribute(CharTermAttribute.class);
    ts.reset();
    for (int tokenNum = 0 ; ts.incrementToken() ; ++tokenNum) {
      assertEquals("token #" + tokenNum + " mismatch: ", termAtt.toString(), tokens.get(tokenNum));
    }
    ts.end();
    ts.close();
  }
}

class SpoonFeedMaxCharsReaderWrapper extends Reader {
  private final Reader in;
  private final int maxChars; 

  public SpoonFeedMaxCharsReaderWrapper(int maxChars, Reader in) {
    this.in = in;
    this.maxChars = maxChars;
  }

  @Override
  public void close() throws IOException {
    in.close();
  }

  /** Returns the configured number of chars if available */
  @Override
  public int read(char[] cbuf, int off, int len) throws IOException {
    return in.read(cbuf, off, Math.min(maxChars, len));
  }
}
