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


import java.io.Reader;
import java.io.StringReader;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.util.BaseTokenStreamFactoryTestCase;

/**
 * A few tests based on org.apache.lucene.analysis.TestUAX29URLEmailTokenizer
 */

public class TestUAX29URLEmailTokenizerFactory extends BaseTokenStreamFactoryTestCase {

  public void testUAX29URLEmailTokenizer() throws Exception {
    Reader reader = new StringReader("Wha\u0301t's this thing do?");
    Tokenizer stream = tokenizerFactory("UAX29URLEmail").create(newAttributeFactory());
    stream.setReader(reader);
    assertTokenStreamContents(stream, 
        new String[] { "Wha\u0301t's", "this", "thing", "do" });
  }
  
  public void testArabic() throws Exception {
    Reader reader = new StringReader("الفيلم الوثائقي الأول عن ويكيبيديا يسمى \"الحقيقة بالأرقام: قصة ويكيبيديا\" (بالإنجليزية: Truth in Numbers: The Wikipedia Story)، سيتم إطلاقه في 2008.");
    Tokenizer stream = tokenizerFactory("UAX29URLEmail").create(newAttributeFactory());
    stream.setReader(reader);
    assertTokenStreamContents(stream, 
        new String[] { "الفيلم", "الوثائقي", "الأول", "عن", "ويكيبيديا", "يسمى", "الحقيقة", "بالأرقام", "قصة", "ويكيبيديا",
        "بالإنجليزية", "Truth", "in", "Numbers", "The", "Wikipedia", "Story", "سيتم", "إطلاقه", "في", "2008"  });
  }
  
  public void testChinese() throws Exception {
    Reader reader = new StringReader("我是中国人。 １２３４ Ｔｅｓｔｓ ");
    Tokenizer stream = tokenizerFactory("UAX29URLEmail").create(newAttributeFactory());
    stream.setReader(reader);
    assertTokenStreamContents(stream, 
        new String[] { "我", "是", "中", "国", "人", "１２３４", "Ｔｅｓｔｓ" });
  }

  public void testKorean() throws Exception {
    Reader reader = new StringReader("안녕하세요 한글입니다");
    Tokenizer stream = tokenizerFactory("UAX29URLEmail").create(newAttributeFactory());
    stream.setReader(reader);
    assertTokenStreamContents(stream, 
        new String[] { "안녕하세요", "한글입니다" });
  }
    
  public void testHyphen() throws Exception {
    Reader reader = new StringReader("some-dashed-phrase");
    Tokenizer stream = tokenizerFactory("UAX29URLEmail").create(newAttributeFactory());
    stream.setReader(reader);
    assertTokenStreamContents(stream, 
        new String[] { "some", "dashed", "phrase" });
  }

  // Test with some URLs from TestUAX29URLEmailTokenizer's 
  // urls.from.random.text.with.urls.txt
  public void testURLs() throws Exception {
    String textWithURLs 
      = "http://johno.jsmf.net/knowhow/ngrams/index.php?table=en-dickens-word-2gram&paragraphs=50&length=200&no-ads=on\n"
        + " some extra\nWords thrown in here. "
        + "http://c5-3486.bisynxu.FR/aI.YnNms/"
        + " samba Halta gamba "
        + "ftp://119.220.152.185/JgJgdZ/31aW5c/viWlfQSTs5/1c8U5T/ih5rXx/YfUJ/xBW1uHrQo6.R\n"
        + "M19nq.0URV4A.Me.CC/mj0kgt6hue/dRXv8YVLOw9v/CIOqb\n"
        + "Https://yu7v33rbt.vC6U3.XN--KPRW13D/y%4fMSzkGFlm/wbDF4m"
        + " inter Locutio "
        + "[c2d4::]/%471j5l/j3KFN%AAAn/Fip-NisKH/\n"
        + "file:///aXvSZS34is/eIgM8s~U5dU4Ifd%c7"
        + " blah Sirrah woof "
        + "http://[a42:a7b6::]/qSmxSUU4z/%52qVl4\n";
    Reader reader = new StringReader(textWithURLs);
    Tokenizer stream = tokenizerFactory("UAX29URLEmail").create(newAttributeFactory());
    stream.setReader(reader);
    assertTokenStreamContents(stream, 
        new String[] { 
          "http://johno.jsmf.net/knowhow/ngrams/index.php?table=en-dickens-word-2gram&paragraphs=50&length=200&no-ads=on",
          "some", "extra", "Words", "thrown", "in", "here",
          "http://c5-3486.bisynxu.FR/aI.YnNms/",
          "samba", "Halta", "gamba",
          "ftp://119.220.152.185/JgJgdZ/31aW5c/viWlfQSTs5/1c8U5T/ih5rXx/YfUJ/xBW1uHrQo6.R",
          "M19nq.0URV4A.Me.CC/mj0kgt6hue/dRXv8YVLOw9v/CIOqb",
          "Https://yu7v33rbt.vC6U3.XN--KPRW13D/y%4fMSzkGFlm/wbDF4m",
          "inter", "Locutio",
          "[c2d4::]/%471j5l/j3KFN%AAAn/Fip-NisKH/",
          "file:///aXvSZS34is/eIgM8s~U5dU4Ifd%c7",
          "blah", "Sirrah", "woof",
          "http://[a42:a7b6::]/qSmxSUU4z/%52qVl4"
        }
    );
  }

  // Test with some emails from TestUAX29URLEmailTokenizer's 
  // email.addresses.from.random.text.with.email.addresses.txt
  public void testEmails() throws Exception {
    String textWithEmails 
      =  " some extra\nWords thrown in here. "
         + "dJ8ngFi@avz13m.CC\n"
         + "kU-l6DS@[082.015.228.189]\n"
         + "\"%U\u0012@?\\B\"@Fl2d.md"
         + " samba Halta gamba "
         + "Bvd#@tupjv.sn\n"
         + "SBMm0Nm.oyk70.rMNdd8k.#ru3LI.gMMLBI.0dZRD4d.RVK2nY@au58t.B13albgy4u.mt\n"
         + "~+Kdz@3mousnl.SE\n"
         + " inter Locutio "
         + "C'ts`@Vh4zk.uoafcft-dr753x4odt04q.UY\n"
         + "}0tzWYDBuy@cSRQAABB9B.7c8xawf75-cyo.PM"
         + " blah Sirrah woof "
         + "lMahAA.j/5.RqUjS745.DtkcYdi@d2-4gb-l6.ae\n"
         + "lv'p@tqk.vj5s0tgl.0dlu7su3iyiaz.dqso.494.3hb76.XN--MGBAAM7A8H\n";
    Reader reader = new StringReader(textWithEmails);
    Tokenizer stream = tokenizerFactory("UAX29URLEmail").create(newAttributeFactory());
    stream.setReader(reader);
    assertTokenStreamContents(stream, 
        new String[] { 
          "some", "extra", "Words", "thrown", "in", "here",
          "dJ8ngFi@avz13m.CC",
          "kU-l6DS@[082.015.228.189]",
          "\"%U\u0012@?\\B\"@Fl2d.md",
          "samba", "Halta", "gamba",
          "Bvd#@tupjv.sn",
          "SBMm0Nm.oyk70.rMNdd8k.#ru3LI.gMMLBI.0dZRD4d.RVK2nY@au58t.B13albgy4u.mt",
          "~+Kdz@3mousnl.SE",
          "inter", "Locutio",
          "C'ts`@Vh4zk.uoafcft-dr753x4odt04q.UY",
          "}0tzWYDBuy@cSRQAABB9B.7c8xawf75-cyo.PM",
          "blah", "Sirrah", "woof",
          "lMahAA.j/5.RqUjS745.DtkcYdi@d2-4gb-l6.ae",
          "lv'p@tqk.vj5s0tgl.0dlu7su3iyiaz.dqso.494.3hb76.XN--MGBAAM7A8H"
        }
    );
  }

  public void testMaxTokenLength() throws Exception {
    StringBuilder builder = new StringBuilder();
    for (int i = 0 ; i < 100 ; ++i) {
      builder.append("abcdefg"); // 7 * 100 = 700 char "word"
    }
    String longWord = builder.toString();
    String content = "one two three " + longWord + " four five six";
    Reader reader = new StringReader(content);
    Tokenizer stream = tokenizerFactory("UAX29URLEmail",
        "maxTokenLength", "1000").create(newAttributeFactory());
    stream.setReader(reader);
    assertTokenStreamContents(stream, 
        new String[] {"one", "two", "three", longWord, "four", "five", "six" });
  }
  
  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      tokenizerFactory("UAX29URLEmail", "bogusArg", "bogusValue");
    });
    assertTrue(expected.getMessage().contains("Unknown parameters"));
  }

  public void testIllegalArguments() throws Exception {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      tokenizerFactory("UAX29URLEmail", "maxTokenLength", "-1").create();
    });
    assertTrue(expected.getMessage().contains("maxTokenLength must be greater than zero"));
  }
}
