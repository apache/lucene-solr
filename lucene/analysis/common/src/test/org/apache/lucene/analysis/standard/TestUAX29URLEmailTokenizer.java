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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.TestUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;


public class TestUAX29URLEmailTokenizer extends BaseTokenStreamTestCase {

  // LUCENE-5440: extremely slow tokenization of text matching email <local-part> (before the '@')
  @Nightly
  public void testLongEMAILatomText() throws Exception {
    // EMAILatomText = [A-Za-z0-9!#$%&'*+-/=?\^_`{|}~]
    char[] emailAtomChars
        = "!#$%&'*+,-./0123456789=?ABCDEFGHIJKLMNOPQRSTUVWXYZ^_`abcdefghijklmnopqrstuvwxyz{|}~".toCharArray();
    StringBuilder builder = new StringBuilder();
    int numChars = TestUtil.nextInt(random(), 100 * 1024, 3 * 1024 * 1024);
    for (int i = 0 ; i < numChars ; ++i) {
      builder.append(emailAtomChars[random().nextInt(emailAtomChars.length)]);
    }
    int tokenCount = 0;
    UAX29URLEmailTokenizer ts = new UAX29URLEmailTokenizer();
    String text = builder.toString();
    ts.setReader(new StringReader(text));
    ts.reset();
    while (ts.incrementToken()) {
      tokenCount++;
    }
    ts.end();
    ts.close();
    assertTrue(tokenCount > 0);

    tokenCount = 0;
    int newBufferSize = TestUtil.nextInt(random(), 200, 8192);
    ts.setMaxTokenLength(newBufferSize);
    ts.setReader(new StringReader(text));
    ts.reset();
    while (ts.incrementToken()) {
      tokenCount++;
    }
    ts.end();
    ts.close();
    assertTrue(tokenCount > 0);
  }

  public void testHugeDoc() throws IOException {
    StringBuilder sb = new StringBuilder();
    char whitespace[] = new char[4094];
    Arrays.fill(whitespace, ' ');
    sb.append(whitespace);
    sb.append("testing 1234");
    String input = sb.toString();
    UAX29URLEmailTokenizer tokenizer = new UAX29URLEmailTokenizer(newAttributeFactory());
    tokenizer.setReader(new StringReader(input));
    BaseTokenStreamTestCase.assertTokenStreamContents(tokenizer, new String[] { "testing", "1234" });
  }

  private Analyzer a, urlAnalyzer, emailAnalyzer;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new UAX29URLEmailTokenizer(newAttributeFactory());
        return new TokenStreamComponents(tokenizer);
      }
    };
    urlAnalyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        UAX29URLEmailTokenizer tokenizer = new UAX29URLEmailTokenizer(newAttributeFactory());
        tokenizer.setMaxTokenLength(UAX29URLEmailTokenizer.MAX_TOKEN_LENGTH_LIMIT);  // Tokenize arbitrary length URLs
        TokenFilter filter = new URLFilter(tokenizer);
        return new TokenStreamComponents(tokenizer, filter);
      }
    };
    emailAnalyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        UAX29URLEmailTokenizer tokenizer = new UAX29URLEmailTokenizer(newAttributeFactory());
        TokenFilter filter = new EmailFilter(tokenizer);
        return new TokenStreamComponents(tokenizer, filter);
      }
    };
  }
  
  @Override
  public void tearDown() throws Exception {
    IOUtils.close(a, urlAnalyzer, emailAnalyzer);
    super.tearDown();
  }

  /** Passes through tokens with type "<URL>" and blocks all other types. */
  private static class URLFilter extends TokenFilter {
    private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);
    public URLFilter(TokenStream in) {
      super(in);
    }
    @Override
    public final boolean incrementToken() throws java.io.IOException {
      boolean isTokenAvailable = false;
      while (input.incrementToken()) {
        if (typeAtt.type() == UAX29URLEmailTokenizer.TOKEN_TYPES[UAX29URLEmailTokenizer.URL]) {
          isTokenAvailable = true;
          break;
        }
      }
      return isTokenAvailable;
    }
  }
  
  /** Passes through tokens with type "<EMAIL>" and blocks all other types. */
  private static class EmailFilter extends TokenFilter {
    private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);
    public EmailFilter(TokenStream in) {
      super(in);
    }
    @Override
    public final boolean incrementToken() throws java.io.IOException {
      boolean isTokenAvailable = false;
      while (input.incrementToken()) {
        if (typeAtt.type() == UAX29URLEmailTokenizer.TOKEN_TYPES[UAX29URLEmailTokenizer.EMAIL]) {
          isTokenAvailable = true;
          break;
        }
      }
      return isTokenAvailable;
    }
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
  
  public void testWikiURLs() throws Exception {
    Reader reader = null;
    String luceneResourcesWikiPage;
    try {
      reader = new InputStreamReader(getClass().getResourceAsStream
        ("LuceneResourcesWikiPage.html"), StandardCharsets.UTF_8);
      StringBuilder builder = new StringBuilder();
      char[] buffer = new char[1024];
      int numCharsRead;
      while (-1 != (numCharsRead = reader.read(buffer))) {
        builder.append(buffer, 0, numCharsRead);
      }
      luceneResourcesWikiPage = builder.toString(); 
    } finally {
      if (null != reader) {
        reader.close();
      }
    }
    assertTrue(null != luceneResourcesWikiPage 
               && luceneResourcesWikiPage.length() > 0);
    BufferedReader bufferedReader = null;
    String[] urls;
    try {
      List<String> urlList = new ArrayList<>();
      bufferedReader = new BufferedReader(new InputStreamReader
        (getClass().getResourceAsStream("LuceneResourcesWikiPageURLs.txt"), StandardCharsets.UTF_8));
      String line;
      while (null != (line = bufferedReader.readLine())) {
        line = line.trim();
        if (line.length() > 0) {
          urlList.add(line);
        }
      }
      urls = urlList.toArray(new String[urlList.size()]);
    } finally {
      if (null != bufferedReader) {
        bufferedReader.close();
      }
    }
    assertTrue(null != urls && urls.length > 0);
    BaseTokenStreamTestCase.assertAnalyzesTo
      (urlAnalyzer, luceneResourcesWikiPage, urls);
  }
  
  public void testEmails() throws Exception {
    Reader reader = null;
    String randomTextWithEmails;
    try {
      reader = new InputStreamReader(getClass().getResourceAsStream
        ("random.text.with.email.addresses.txt"), StandardCharsets.UTF_8);
      StringBuilder builder = new StringBuilder();
      char[] buffer = new char[1024];
      int numCharsRead;
      while (-1 != (numCharsRead = reader.read(buffer))) {
        builder.append(buffer, 0, numCharsRead);
      }
      randomTextWithEmails = builder.toString(); 
    } finally {
      if (null != reader) {
        reader.close();
      }
    }
    assertTrue(null != randomTextWithEmails 
               && randomTextWithEmails.length() > 0);
    BufferedReader bufferedReader = null;
    String[] emails;
    try {
      List<String> emailList = new ArrayList<>();
      bufferedReader = new BufferedReader(new InputStreamReader
        (getClass().getResourceAsStream
          ("email.addresses.from.random.text.with.email.addresses.txt"), StandardCharsets.UTF_8));
      String line;
      while (null != (line = bufferedReader.readLine())) {
        line = line.trim();
        if (line.length() > 0) {
          emailList.add(line);
        }
      }
      emails = emailList.toArray(new String[emailList.size()]);
    } finally {
      if (null != bufferedReader) {
        bufferedReader.close();
      }
    }
    assertTrue(null != emails && emails.length > 0);
    BaseTokenStreamTestCase.assertAnalyzesTo
      (emailAnalyzer, randomTextWithEmails, emails);
  }

  public void testMailtoSchemeEmails () throws Exception {
    // See LUCENE-3880
    BaseTokenStreamTestCase.assertAnalyzesTo(a, "mailto:test@example.org",
        new String[] {"mailto", "test@example.org"},
        new String[] { "<ALPHANUM>", "<EMAIL>" });

    // TODO: Support full mailto: scheme URIs. See RFC 6068: http://tools.ietf.org/html/rfc6068
    BaseTokenStreamTestCase.assertAnalyzesTo
        (a,  "mailto:personA@example.com,personB@example.com?cc=personC@example.com"
           + "&subject=Subjectivity&body=Corpusivity%20or%20something%20like%20that",
         new String[] { "mailto",
                        "personA@example.com",
                        // TODO: recognize ',' address delimiter. Also, see examples of ';' delimiter use at: http://www.mailto.co.uk/
                        ",personB@example.com",
                        "?cc=personC@example.com", // TODO: split field keys/values
                        "subject", "Subjectivity",
                        "body", "Corpusivity", "20or", "20something","20like", "20that" }, // TODO: Hex decoding + re-tokenization
         new String[] { "<ALPHANUM>",
                        "<EMAIL>",
                        "<EMAIL>",
                        "<EMAIL>",
                        "<ALPHANUM>", "<ALPHANUM>",
                        "<ALPHANUM>", "<ALPHANUM>", "<ALPHANUM>", "<ALPHANUM>", "<ALPHANUM>", "<ALPHANUM>" });
  }

  public void testURLs() throws Exception {
    Reader reader = null;
    String randomTextWithURLs;
    try {
      reader = new InputStreamReader(getClass().getResourceAsStream
        ("random.text.with.urls.txt"), StandardCharsets.UTF_8);
      StringBuilder builder = new StringBuilder();
      char[] buffer = new char[1024];
      int numCharsRead;
      while (-1 != (numCharsRead = reader.read(buffer))) {
        builder.append(buffer, 0, numCharsRead);
      }
      randomTextWithURLs = builder.toString(); 
    } finally {
      if (null != reader) {
        reader.close();
      }
    }
    assertTrue(null != randomTextWithURLs 
               && randomTextWithURLs.length() > 0);
    BufferedReader bufferedReader = null;
    String[] urls;
    try {
      List<String> urlList = new ArrayList<>();
      bufferedReader = new BufferedReader(new InputStreamReader
        (getClass().getResourceAsStream
          ("urls.from.random.text.with.urls.txt"), StandardCharsets.UTF_8));
      String line;
      while (null != (line = bufferedReader.readLine())) {
        line = line.trim();
        if (line.length() > 0) {
          urlList.add(line);
        }
      }
      urls = urlList.toArray(new String[urlList.size()]);
    } finally {
      if (null != bufferedReader) {
        bufferedReader.close();
      }
    }
    assertTrue(null != urls && urls.length > 0);
    BaseTokenStreamTestCase.assertAnalyzesTo
      (urlAnalyzer, randomTextWithURLs, urls);
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
   * Multiple consecutive chars in \p{Word_Break = MidLetter},
   * \p{Word_Break = MidNumLet}, and/or \p{Word_Break = MidNum}
   * should trigger a token split.
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
    checkRandomData(random(), a, 200 * RANDOM_MULTIPLIER);
  }
  
  /** blast some random large strings through the analyzer */
  public void testRandomHugeStrings() throws Exception {
    Random random = random();
    checkRandomData(random, a, 3 * RANDOM_MULTIPLIER, 8192);
  }

  public void testExampleURLs() throws Exception {
    String[] TLDs = {
        "aaa", "ac", "ai", "aarp", "abarth", "abb", "abbott", "abbvie", "abc", "able", "abogado", "abudhabi", 
        "academy", "accenture", "accountant", "accountants", "aco", "active", "actor", "ad", "adac", "ads", "adult", 
        "ae", "aeg", "aero", "aetna", "af", "afamilycompany", "afl", "africa", "ag", "agakhan", "agency", "aig", 
        "aigo", "airbus", "airforce", "airtel", "akdn", "al", "alfaromeo", "alibaba", "alipay", "allfinanz", 
        "allstate", "ally", "alsace", "alstom", "am", "americanexpress", "americanfamily", "amex", "amfam", "amica", 
        "amsterdam", "analytics", "android", "anquan", "anz", "ao", "aol", "apartments", "app", "apple", "aq", 
        "aquarelle", "ar", "arab", "aramco", "archi", "army", "arpa", "art", "arte", "as", "asda", "asia", 
        "associates", "at", "athleta", "attorney", "au", "auction", "audi", "audible", "audio", "auspost", "author", 
        "auto", "autos", "avianca", "aw", "aws", "ax", "axa", "az", "azure", "ba", "baby", "baidu", "banamex", 
        "bananarepublic", "band", "bank", "bar", "barcelona", "barclaycard", "barclays", "barefoot", "bargains", 
        "baseball", "basketball", "bauhaus", "bayern", "bb", "bbc", "bbt", "bbva", "bcg", "bcn", "bd", "be", "beats",
        "beauty", "beer", "bentley", "berlin", "best", "bestbuy", "bet", "bf", "bg", "bh", "bharti", "bi", "bible", 
        "bid", "bike", "bing", "bingo", "bio", "biz", "bj", "black", "blackfriday", "blanco", "blockbuster", "blog", 
        "bloomberg", "blue", "bm", "bms", "bmw", "bn", "bnl", "bnpparibas", "bo", "boats", "boehringer", "bofa", 
        "bom", "bond", "boo", "book", "booking", "bosch", "bostik", "boston", "bot", "boutique", "box", "br", 
        "bradesco", "bridgestone", "broadway", "broker", "brother", "brussels", "bs", "bt", "budapest", "bugatti", 
        "build", "builders", "business", "buy", "buzz", "bv", "bw", "by", "bz", "bzh", "ca", "cab", "cafe", "cal", 
        "call", "calvinklein", "cam", "camera", "camp", "cancerresearch", "canon", "capetown", "capital", 
        "capitalone", "car", "caravan", "cards", "care", "career", "careers", "cars", "cartier", "casa", "case", 
        "caseih", "cash", "casino", "cat", "catering", "catholic", "cba", "cbn", "cbre", "cbs", "cc", "cd", "ceb", 
        "center", "ceo", "cern", "cf", "cfa", "cfd", "cg", "ch", "chanel", "channel", "chase", "chat", "cheap", 
        "chintai", "christmas", "chrome", "chrysler", "church", "ci", "cipriani", "circle", "cisco", "citadel", 
        "citi", "citic", "city", "cityeats", "ck", "cl", "claims", "cleaning", "click", "clinic", "clinique", 
        "clothing", "cloud", "club", "clubmed", "cm", "cn", "co", "coach", "codes", "coffee", "college", "cologne", 
        "com", "comcast", "commbank", "community", "company", "compare", "computer", "comsec", "condos", 
        "construction", "consulting", "contact", "contractors", "cooking", "cookingchannel", "cool", "coop", 
        "corsica", "country", "coupon", "coupons", "courses", "cr", "credit", "creditcard", "creditunion", "cricket",
        "crown", "crs", "cruise", "cruises", "csc", "cu", "cuisinella", "cv", "cw", "cx", "cy", "cymru", "cyou", 
        "cz", "dabur", "dad", "dance", "data", "date", "dating", "datsun", "day", "dclk", "dds", "de", "deal", 
        "dealer", "deals", "degree", "delivery", "dell", "deloitte", "delta", "democrat", "dental", "dentist", 
        "desi", "design", "dev", "dhl", "diamonds", "diet", "digital", "direct", "directory", "discount", "discover",
        "dish", "diy", "dj", "dk", "dm", "dnp", "do", "docs", "doctor", "dodge", "dog", "doha", "domains", "dot", 
        "download", "drive", "dtv", "dubai", "duck", "dunlop", "duns", "dupont", "durban", "dvag", "dvr", "dz", 
        "earth", "eat", "ec", "eco", "edeka", "edu", "education", "ee", "eg", "email", "emerck", "energy", 
        "engineer", "engineering", "enterprises", "epost", "epson", "equipment", "er", "ericsson", "erni", "es", 
        "esq", "estate", "esurance", "et", "etisalat", "eu", "eurovision", "eus", "events", "everbank", "exchange", 
        "expert", "exposed", "express", "extraspace", "fage", "fail", "fairwinds", "faith", "family", "fan", "fans", 
        "farm", "farmers", "fashion", "fast", "fedex", "feedback", "ferrari", "ferrero", "fi", "fiat", "fidelity", 
        "fido", "film", "final", "finance", "financial", "fire", "firestone", "firmdale", "fish", "fishing", "fit", 
        "fitness", "fj", "fk", "flickr", "flights", "flir", "florist", "flowers", "fly", "fm", "fo", "foo", "food", 
        "foodnetwork", "football", "ford", "forex", "forsale", "forum", "foundation", "fox", "fr", "free", 
        "fresenius", "frl", "frogans", "frontdoor", "frontier", "ftr", "fujitsu", "fujixerox", "fun", "fund", 
        "furniture", "futbol", "fyi", "ga", "gal", "gallery", "gallo", "gallup", "game", "games", "gap", "garden", 
        "gb", "gbiz", "gd", "gdn", "ge", "gea", "gent", "genting", "george", "gf", "gg", "ggee", "gh", "gi", "gift", 
        "gifts", "gives", "giving", "gl", "glade", "glass", "gle", "global", "globo", "gm", "gmail", "gmbh", "gmo", 
        "gmx", "gn", "godaddy", "gold", "goldpoint", "golf", "goo", "goodhands", "goodyear", "goog", "google", "gop",
        "got", "gov", "gp", "gq", "gr", "grainger", "graphics", "gratis", "green", "gripe", "grocery", "group", "gs",
        "gt", "gu", "guardian", "gucci", "guge", "guide", "guitars", "guru", "gw", "gy", "hair", "hamburg", 
        "hangout", "haus", "hbo", "hdfc", "hdfcbank", "health", "healthcare", "help", "helsinki", "here", "hermes", 
        "hgtv", "hiphop", "hisamitsu", "hitachi", "hiv", "hk", "hkt", "hm", "hn", "hockey", "holdings", "holiday", 
        "homedepot", "homegoods", "homes", "homesense", "honda", "honeywell", "horse", "hospital", "host", "hosting",
        "hot", "hoteles", "hotels", "hotmail", "house", "how", "hr", "hsbc", "ht", "hu", "hughes", "hyatt", 
        "hyundai", "ibm", "icbc", "ice", "icu", "id", "ie", "ieee", "ifm", "ikano", "il", "im", "imamat", "imdb", 
        "immo", "immobilien", "in", "industries", "infiniti", "info", "ing", "ink", "institute", "insurance", 
        "insure", "int", "intel", "international", "intuit", "investments", "io", "ipiranga", "iq", "ir", "irish", 
        "is", "iselect", "ismaili", "ist", "istanbul", "it", "itau", "itv", "iveco", "iwc", "jaguar", "java", "jcb", 
        "jcp", "je", "jeep", "jetzt", "jewelry", "jio", "jlc", "jll", "jm", "jmp", "jnj", "jo", "jobs", "joburg", 
        "jot", "joy", "jp", "jpmorgan", "jprs", "juegos", "juniper", "kaufen", "kddi", "ke", "kerryhotels", 
        "kerrylogistics", "kerryproperties", "kfh", "kg", "kh", "ki", "kia", "kim", "kinder", "kindle", "kitchen", 
        "kiwi", "km", "kn", "koeln", "komatsu", "kosher", "kp", "kpmg", "kpn", "kr", "krd", "kred", "kuokgroup", 
        "kw", "ky", "kyoto", "kz", "la", "lacaixa", "ladbrokes", "lamborghini", "lamer", "lancaster", "lancia", 
        "lancome", "land", "landrover", "lanxess", "lasalle", "lat", "latino", "latrobe", "law", "lawyer", "lb", 
        "lc", "lds", "lease", "leclerc", "lefrak", "legal", "lego", "lexus", "lgbt", "li", "liaison", "lidl", "life",
        "lifeinsurance", "lifestyle", "lighting", "like", "lilly", "limited", "limo", "lincoln", "linde", "link", 
        "lipsy", "live", "living", "lixil", "lk", "llc", "loan", "loans", "locker", "locus", "loft", "lol", "london",
        "lotte", "lotto", "love", "lpl", "lplfinancial", "lr", "ls", "lt", "ltd", "ltda", "lu", "lundbeck", "lupin", 
        "luxe", "luxury", "lv", "ly", "ma", "macys", "madrid", "maif", "maison", "makeup", "man", "management", 
        "mango", "map", "market", "marketing", "markets", "marriott", "marshalls", "maserati", "mattel", "mba", "mc",
        "mckinsey", "md", "me", "med", "media", "meet", "melbourne", "meme", "memorial", "men", "menu", "merckmsd", 
        "metlife", "mg", "mh", "miami", "microsoft", "mil", "mini", "mint", "mit", "mitsubishi", "mk", "ml", "mlb", 
        "mls", "mm", "mma", "mn", "mo", "mobi", "mobile", "mobily", "moda", "moe", "moi", "mom", "monash", "money", 
        "monster", "mopar", "mormon", "mortgage", "moscow", "moto", "motorcycles", "mov", "movie", "movistar", "mp", 
        "mq", "mr", "ms", "msd", "mt", "mtn", "mtr", "mu", "museum", "mutual", "mv", "mw", "mx", "my", "mz", "na", 
        "nab", "nadex", "nagoya", "name", "nationwide", "natura", "navy", "nba", "nc", "ne", "nec", "net", "netbank",
        "netflix", "network", "neustar", "new", "newholland", "news", "next", "nextdirect", "nexus", "nf", "nfl", 
        "ng", "ngo", "nhk", "ni", "nico", "nike", "nikon", "ninja", "nissan", "nissay", "nl", "no", "nokia", 
        "northwesternmutual", "norton", "now", "nowruz", "nowtv", "np", "nr", "nra", "nrw", "ntt", "nu", "nyc", "nz",
        "obi", "observer", "off", "office", "okinawa", "olayan", "olayangroup", "oldnavy", "ollo", "om", "omega", 
        "one", "ong", "onl", "online", "onyourside", "ooo", "open", "oracle", "orange", "org", "organic", "origins", 
        "osaka", "otsuka", "ott", "ovh", "pa", "page", "panasonic", "panerai", "paris", "pars", "partners", "parts", 
        "party", "passagens", "pay", "pccw", "pe", "pet", "pf", "pfizer", "pg", "ph", "pharmacy", "phd", "philips", 
        "phone", "photo", "photography", "photos", "physio", "piaget", "pics", "pictet", "pictures", "pid", "pin", 
        "ping", "pink", "pioneer", "pizza", "pk", "pl", "place", "play", "playstation", "plumbing", "plus", "pm", 
        "pn", "pnc", "pohl", "poker", "politie", "porn", "post", "pr", "pramerica", "praxi", "press", "prime", "pro",
        "prod", "productions", "prof", "progressive", "promo", "properties", "property", "protection", "pru", 
        "prudential", "ps", "pt", "pub", "pw", "pwc", "py", "qa", "qpon", "quebec", "quest", "qvc", "racing", 
        "radio", "raid", "re", "read", "realestate", "realtor", "realty", "recipes", "red", "redstone", 
        "redumbrella", "rehab", "reise", "reisen", "reit", "reliance", "ren", "rent", "rentals", "repair", "report", 
        "republican", "rest", "restaurant", "review", "reviews", "rexroth", "rich", "richardli", "ricoh", 
        "rightathome", "ril", "rio", "rip", "rmit", "ro", "rocher", "rocks", "rodeo", "rogers", "room", "rs", "rsvp",
        "ru", "rugby", "ruhr", "run", "rw", "rwe", "ryukyu", "sa", "saarland", "safe", "safety", "sakura", "sale", 
        "salon", "samsclub", "samsung", "sandvik", "sandvikcoromant", "sanofi", "sap", "sarl", "sas", "save", "saxo",
        "sb", "sbi", "sbs", "sc", "sca", "scb", "schaeffler", "schmidt", "scholarships", "school", "schule", 
        "schwarz", "science", "scjohnson", "scor", "scot", "sd", "se", "search", "seat", "secure", "security", 
        "seek", "select", "sener", "services", "ses", "seven", "sew", "sex", "sexy", "sfr", "sg", "sh", "shangrila", 
        "sharp", "shaw", "shell", "shia", "shiksha", "shoes", "shop", "shopping", "shouji", "show", "showtime", 
        "shriram", "si", "silk", "sina", "singles", "site", "sj", "sk", "ski", "skin", "sky", "skype", "sl", "sling",
        "sm", "smart", "smile", "sn", "sncf", "so", "soccer", "social", "softbank", "software", "sohu", "solar", 
        "solutions", "song", "sony", "soy", "space", "spiegel", "sport", "spot", "spreadbetting", "sr", "srl", "srt",
        "st", "stada", "staples", "star", "starhub", "statebank", "statefarm", "statoil", "stc", "stcgroup", 
        "stockholm", "storage", "store", "stream", "studio", "study", "style", "su", "sucks", "supplies", "supply", 
        "support", "surf", "surgery", "suzuki", "sv", "swatch", "swiftcover", "swiss", "sx", "sy", "sydney", 
        "symantec", "systems", "sz", "tab", "taipei", "talk", "taobao", "target", "tatamotors", "tatar", "tattoo", 
        "tax", "taxi", "tc", "tci", "td", "tdk", "team", "tech", "technology", "tel", "telecity", "telefonica", 
        "temasek", "tennis", "teva", "tf", "tg", "th", "thd", "theater", "theatre", "tiaa", "tickets", "tienda", 
        "tiffany", "tips", "tires", "tirol", "tj", "tjmaxx", "tjx", "tk", "tkmaxx", "tl", "tm", "tmall", "tn", "to", 
        "today", "tokyo", "tools", "top", "toray", "toshiba", "total", "tours", "town", "toyota", "toys", "tr", 
        "trade", "trading", "training", "travel", "travelchannel", "travelers", "travelersinsurance", "trust", "trv",
        "tt", "tube", "tui", "tunes", "tushu", "tv", "tvs", "tw", "tz", "ua", "ubank", "ubs", "uconnect", "ug", "uk",
        "unicom", "university", "uno", "uol", "ups", "us", "uy", "uz", "va", "vacations", "vana", "vanguard", "vc", 
        "ve", "vegas", "ventures", "verisign", "versicherung", "vet", "vg", "vi", "viajes", "video", "vig", "viking",
        "villas", "vin", "vip", "virgin", "visa", "vision", "vista", "vistaprint", "viva", "vivo", "vlaanderen", 
        "vn", "vodka", "volkswagen", "volvo", "vote", "voting", "voto", "voyage", "vu", "vuelos", "wales", "walmart",
        "walter", "wang", "wanggou", "warman", "watch", "watches", "weather", "weatherchannel", "webcam", "weber", 
        "website", "wed", "wedding", "weibo", "weir", "wf", "whoswho", "wien", "wiki", "williamhill", "win", 
        "windows", "wine", "winners", "wme", "wolterskluwer", "woodside", "work", "works", "world", "wow", "ws", 
        "wtc", "wtf", "xbox", "xerox", "xfinity", "xihuan", "xin", "xn--11b4c3d", "xn--1ck2e1b", "xn--1qqw23a", 
        "xn--2scrj9c", "xn--30rr7y", "xn--3bst00m", "xn--3ds443g", "xn--3e0b707e", "xn--3hcrj9c", 
        "xn--3oq18vl8pn36a", "xn--3pxu8k", "xn--42c2d9a", "xn--45br5cyl", "xn--45brj9c", "xn--45q11c", "xn--4gbrim", 
        "xn--54b7fta0cc", "xn--55qw42g", "xn--55qx5d", "xn--5su34j936bgsg", "xn--5tzm5g", "xn--6frz82g", 
        "xn--6qq986b3xl", "xn--80adxhks", "xn--80ao21a", "xn--80aqecdr1a", "xn--80asehdb", "xn--80aswg", 
        "xn--8y0a063a", "xn--90a3ac", "xn--90ae", "xn--90ais", "xn--9dbq2a", "xn--9et52u", "xn--9krt00a", 
        "xn--b4w605ferd", "xn--bck1b9a5dre4c", "xn--c1avg", "xn--c2br7g", "xn--cck2b3b", "xn--cg4bki", 
        "xn--clchc0ea0b2g2a9gcd", "xn--czr694b", "xn--czrs0t", "xn--czru2d", "xn--d1acj3b", "xn--d1alf", "xn--e1a4c",
        "xn--eckvdtc9d", "xn--efvy88h", "xn--estv75g", "xn--fct429k", "xn--fhbei", "xn--fiq228c5hs", "xn--fiq64b", 
        "xn--fiqs8s", "xn--fiqz9s", "xn--fjq720a", "xn--flw351e", "xn--fpcrj9c3d", "xn--fzc2c9e2c", 
        "xn--fzys8d69uvgm", "xn--g2xx48c", "xn--gckr3f0f", "xn--gecrj9c", "xn--gk3at1e", "xn--h2breg3eve", 
        "xn--h2brj9c", "xn--h2brj9c8c", "xn--hxt814e", "xn--i1b6b1a6a2e", "xn--imr513n", "xn--io0a7i", "xn--j1aef", 
        "xn--j1amh", "xn--j6w193g", "xn--jlq61u9w7b", "xn--jvr189m", "xn--kcrx77d1x4a", "xn--kprw13d", "xn--kpry57d",
        "xn--kpu716f", "xn--kput3i", "xn--l1acc", "xn--lgbbat1ad8j", "xn--mgb9awbf", "xn--mgba3a3ejt", 
        "xn--mgba3a4f16a", "xn--mgba7c0bbn0a", "xn--mgbaakc7dvf", "xn--mgbaam7a8h", "xn--mgbab2bd", 
        "xn--mgbai9azgqp6j", "xn--mgbayh7gpa", "xn--mgbb9fbpob", "xn--mgbbh1a", "xn--mgbbh1a71e", "xn--mgbc0a9azcg", 
        "xn--mgbca7dzdo", "xn--mgberp4a5d4ar", "xn--mgbgu82a", "xn--mgbi4ecexp", "xn--mgbpl2fh", "xn--mgbt3dhd", 
        "xn--mgbtx2b", "xn--mgbx4cd0ab", "xn--mix891f", "xn--mk1bu44c", "xn--mxtq1m", "xn--ngbc5azd", "xn--ngbe9e0a",
        "xn--ngbrx", "xn--node", "xn--nqv7f", "xn--nqv7fs00ema", "xn--nyqy26a", "xn--o3cw4h", "xn--ogbpf8fl", 
        "xn--otu796d", "xn--p1acf", "xn--p1ai", "xn--pbt977c", "xn--pgbs0dh", "xn--pssy2u", "xn--q9jyb4c", 
        "xn--qcka1pmc", "xn--qxam", "xn--rhqv96g", "xn--rovu88b", "xn--rvc1e0am3e", "xn--s9brj9c", "xn--ses554g", 
        "xn--t60b56a", "xn--tckwe", "xn--tiq49xqyj", "xn--unup4y", "xn--vermgensberater-ctb", 
        "xn--vermgensberatung-pwb", "xn--vhquv", "xn--vuq861b", "xn--w4r85el8fhu5dnra", "xn--w4rs40l", "xn--wgbh1c", 
        "xn--wgbl6a", "xn--xhq521b", "xn--xkc2al3hye2a", "xn--xkc2dl3a5ee0h", "xn--y9a3aq", "xn--yfro4i67o", 
        "xn--ygbi2ammx", "xn--zfr164b", "xperia", "xxx", "xyz", "yachts", "yahoo", "yamaxun", "yandex", "ye", 
        "yodobashi", "yoga", "yokohama", "you", "youtube", "yt", "yun", "za", "zappos", "zara", "zero", "zip", 
        "zippo", "zm", "zone", "zuerich", "zw"
    };

    Analyzer analyzer = new Analyzer() {
      @Override protected TokenStreamComponents createComponents(String fieldName) {
        return new TokenStreamComponents(new UAX29URLEmailTokenizer(newAttributeFactory()));
    }};
    
    for (String tld : TLDs) {
      String URL = "example." + tld;
      BaseTokenStreamTestCase.assertAnalyzesTo(analyzer, URL, new String[]{URL}, new String[]{"<URL>"});
    }
  }
}
