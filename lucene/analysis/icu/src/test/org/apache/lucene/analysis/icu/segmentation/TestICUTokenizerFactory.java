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


import java.io.Reader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.util.ClasspathResourceLoader;

/** basic tests for {@link ICUTokenizerFactory} **/
public class TestICUTokenizerFactory extends BaseTokenStreamTestCase {
  public void testMixedText() throws Exception {
    Reader reader = new StringReader("การที่ได้ต้องแสดงว่างานดี  This is a test ກວ່າດອກ");
    ICUTokenizerFactory factory = new ICUTokenizerFactory(new HashMap<String,String>());
    factory.inform(new ClasspathResourceLoader(getClass()));
    Tokenizer stream = factory.create(newAttributeFactory());
    stream.setReader(reader);
    assertTokenStreamContents(stream,
        new String[] { "การ", "ที่", "ได้", "ต้อง", "แสดง", "ว่า", "งาน", "ดี",
        "This", "is", "a", "test", "ກວ່າ", "ດອກ"});
  }

  public void testTokenizeLatinOnWhitespaceOnly() throws Exception {
    // “ U+201C LEFT DOUBLE QUOTATION MARK; ” U+201D RIGHT DOUBLE QUOTATION MARK
    Reader reader = new StringReader
        ("  Don't,break.at?/(punct)!  \u201Cnice\u201D\r\n\r\n85_At:all; `really\" +2=3$5,&813 !@#%$^)(*@#$   ");
    final Map<String,String> args = new HashMap<>();
    args.put(ICUTokenizerFactory.RULEFILES, "Latn:Latin-break-only-on-whitespace.rbbi");
    ICUTokenizerFactory factory = new ICUTokenizerFactory(args);
    factory.inform(new ClasspathResourceLoader(this.getClass()));
    Tokenizer stream = factory.create(newAttributeFactory());
    stream.setReader(reader);
    assertTokenStreamContents(stream,
        new String[] { "Don't,break.at?/(punct)!", "\u201Cnice\u201D", "85_At:all;", "`really\"",  "+2=3$5,&813", "!@#%$^)(*@#$" },
        new String[] { "<ALPHANUM>",               "<ALPHANUM>",       "<ALPHANUM>", "<ALPHANUM>", "<NUM>",       "<OTHER>" });
  }

  public void testTokenizeLatinDontBreakOnHyphens() throws Exception {
    Reader reader = new StringReader
        ("One-two punch.  Brang-, not brung-it.  This one--not that one--is the right one, -ish.");
    final Map<String,String> args = new HashMap<>();
    args.put(ICUTokenizerFactory.RULEFILES, "Latn:Latin-dont-break-on-hyphens.rbbi");
    ICUTokenizerFactory factory = new ICUTokenizerFactory(args);
    factory.inform(new ClasspathResourceLoader(getClass()));
    Tokenizer stream = factory.create(newAttributeFactory());
    stream.setReader(reader);
    assertTokenStreamContents(stream,
        new String[] { "One-two", "punch",
            "Brang", "not", "brung-it",
            "This", "one", "not", "that", "one", "is", "the", "right", "one", "ish" });
  }

  /**
   * Specify more than one script/rule file pair.
   * Override default DefaultICUTokenizerConfig Thai script tokenization.
   * Use the same rule file for both scripts.
   */
  public void testKeywordTokenizeCyrillicAndThai() throws Exception {
    Reader reader = new StringReader
        ("Some English.  Немного русский.  ข้อความภาษาไทยเล็ก ๆ น้อย ๆ  More English.");
    final Map<String,String> args = new HashMap<>();
    args.put(ICUTokenizerFactory.RULEFILES, "Cyrl:KeywordTokenizer.rbbi,Thai:KeywordTokenizer.rbbi");
    ICUTokenizerFactory factory = new ICUTokenizerFactory(args);
    factory.inform(new ClasspathResourceLoader(getClass()));
    Tokenizer stream = factory.create(newAttributeFactory());
    stream.setReader(reader);
    assertTokenStreamContents(stream, new String[] { "Some", "English",
        "Немного русский.  ",
        "ข้อความภาษาไทยเล็ก ๆ น้อย ๆ  ",
        "More", "English" });
  }
  
  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    try {
      new ICUTokenizerFactory(new HashMap<String,String>() {{
        put("bogusArg", "bogusValue");
      }});
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Unknown parameters"));
    }
  }
}
