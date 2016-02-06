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

package org.apache.lucene.analysis.wikipedia;

import java.io.StringReader;
import java.io.IOException;
import java.util.Collections;
import java.util.Random;
import java.util.Set;
import java.util.HashSet;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.FlagsAttribute;

import static org.apache.lucene.analysis.wikipedia.WikipediaTokenizer.*;

/**
 * Basic Tests for {@link WikipediaTokenizer}
 **/
public class WikipediaTokenizerTest extends BaseTokenStreamTestCase {
  protected static final String LINK_PHRASES = "click [[link here again]] click [http://lucene.apache.org here again] [[Category:a b c d]]";

  public void testSimple() throws Exception {
    String text = "This is a [[Category:foo]]";
    WikipediaTokenizer tf = new WikipediaTokenizer(newAttributeFactory(), WikipediaTokenizer.TOKENS_ONLY, Collections.<String>emptySet());
    tf.setReader(new StringReader(text));
    assertTokenStreamContents(tf,
        new String[] { "This", "is", "a", "foo" },
        new int[] { 0, 5, 8, 21 },
        new int[] { 4, 7, 9, 24 },
        new String[] { "<ALPHANUM>", "<ALPHANUM>", "<ALPHANUM>", CATEGORY },
        new int[] { 1, 1, 1, 1, },
        text.length());
  }
  
  public void testHandwritten() throws Exception {
    // make sure all tokens are in only one type
    String test = "[[link]] This is a [[Category:foo]] Category  This is a linked [[:Category:bar none withstanding]] "
        + "Category This is (parens) This is a [[link]]  This is an external URL [http://lucene.apache.org] "
        + "Here is ''italics'' and ''more italics'', '''bold''' and '''''five quotes''''' "
        + " This is a [[link|display info]]  This is a period.  Here is $3.25 and here is 3.50.  Here's Johnny.  "
        + "==heading== ===sub head=== followed by some text  [[Category:blah| ]] "
        + "''[[Category:ital_cat]]''  here is some that is ''italics [[Category:foo]] but is never closed."
        + "'''same [[Category:foo]] goes for this '''''and2 [[Category:foo]] and this"
        + " [http://foo.boo.com/test/test/ Test Test] [http://foo.boo.com/test/test/test.html Test Test]"
        + " [http://foo.boo.com/test/test/test.html?g=b&c=d Test Test] <ref>Citation</ref> <sup>martian</sup> <span class=\"glue\">code</span>";
    
    WikipediaTokenizer tf = new WikipediaTokenizer(newAttributeFactory(), WikipediaTokenizer.TOKENS_ONLY, Collections.<String>emptySet());
    tf.setReader(new StringReader(test));
    assertTokenStreamContents(tf, 
      new String[] {"link", "This", "is", "a",
        "foo", "Category", "This", "is", "a", "linked", "bar", "none",
        "withstanding", "Category", "This", "is", "parens", "This", "is", "a",
        "link", "This", "is", "an", "external", "URL",
        "http://lucene.apache.org", "Here", "is", "italics", "and", "more",
        "italics", "bold", "and", "five", "quotes", "This", "is", "a", "link",
        "display", "info", "This", "is", "a", "period", "Here", "is", "3.25",
        "and", "here", "is", "3.50", "Here's", "Johnny", "heading", "sub",
        "head", "followed", "by", "some", "text", "blah", "ital", "cat",
        "here", "is", "some", "that", "is", "italics", "foo", "but", "is",
        "never", "closed", "same", "foo", "goes", "for", "this", "and2", "foo",
        "and", "this", "http://foo.boo.com/test/test/", "Test", "Test",
        "http://foo.boo.com/test/test/test.html", "Test", "Test",
        "http://foo.boo.com/test/test/test.html?g=b&c=d", "Test", "Test",
        "Citation", "martian", "code"}, 
      new String[] {INTERNAL_LINK,
        "<ALPHANUM>", "<ALPHANUM>", "<ALPHANUM>", CATEGORY, "<ALPHANUM>",
        "<ALPHANUM>", "<ALPHANUM>", "<ALPHANUM>", "<ALPHANUM>", CATEGORY,
        CATEGORY, CATEGORY, "<ALPHANUM>", "<ALPHANUM>", "<ALPHANUM>",
        "<ALPHANUM>", "<ALPHANUM>", "<ALPHANUM>", "<ALPHANUM>", INTERNAL_LINK,
        "<ALPHANUM>", "<ALPHANUM>", "<ALPHANUM>", "<ALPHANUM>", "<ALPHANUM>",
        EXTERNAL_LINK_URL, "<ALPHANUM>", "<ALPHANUM>", ITALICS, "<ALPHANUM>",
        ITALICS, ITALICS, BOLD, "<ALPHANUM>", BOLD_ITALICS, BOLD_ITALICS,
        "<ALPHANUM>", "<ALPHANUM>", "<ALPHANUM>", INTERNAL_LINK, INTERNAL_LINK,
        INTERNAL_LINK, "<ALPHANUM>", "<ALPHANUM>", "<ALPHANUM>", "<ALPHANUM>",
        "<ALPHANUM>", "<ALPHANUM>", "<NUM>", "<ALPHANUM>", "<ALPHANUM>",
        "<ALPHANUM>", "<NUM>", "<APOSTROPHE>", "<ALPHANUM>", HEADING,
        SUB_HEADING, SUB_HEADING, "<ALPHANUM>", "<ALPHANUM>", "<ALPHANUM>",
        "<ALPHANUM>", CATEGORY, CATEGORY, CATEGORY, "<ALPHANUM>", "<ALPHANUM>",
        "<ALPHANUM>", "<ALPHANUM>", "<ALPHANUM>", ITALICS, CATEGORY,
        "<ALPHANUM>", "<ALPHANUM>", "<ALPHANUM>", "<ALPHANUM>", BOLD, CATEGORY,
        "<ALPHANUM>", "<ALPHANUM>", "<ALPHANUM>", BOLD_ITALICS, CATEGORY,
        "<ALPHANUM>", "<ALPHANUM>", EXTERNAL_LINK_URL, EXTERNAL_LINK,
        EXTERNAL_LINK, EXTERNAL_LINK_URL, EXTERNAL_LINK, EXTERNAL_LINK,
        EXTERNAL_LINK_URL, EXTERNAL_LINK, EXTERNAL_LINK, CITATION,
        "<ALPHANUM>", "<ALPHANUM>"});
  }

  public void testLinkPhrases() throws Exception {
    WikipediaTokenizer tf = new WikipediaTokenizer(newAttributeFactory(), WikipediaTokenizer.TOKENS_ONLY, Collections.<String>emptySet());
    tf.setReader(new StringReader(LINK_PHRASES));
    checkLinkPhrases(tf);
  }

  private void checkLinkPhrases(WikipediaTokenizer tf) throws IOException {
    assertTokenStreamContents(tf,
        new String[] { "click", "link", "here", "again", "click", 
        "http://lucene.apache.org", "here", "again", "a", "b", "c", "d" },
        new int[] { 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1, 1 });
  }

  public void testLinks() throws Exception {
    String test = "[http://lucene.apache.org/java/docs/index.html#news here] [http://lucene.apache.org/java/docs/index.html?b=c here] [https://lucene.apache.org/java/docs/index.html?b=c here]";
    WikipediaTokenizer tf = new WikipediaTokenizer(newAttributeFactory(), WikipediaTokenizer.TOKENS_ONLY, Collections.<String>emptySet());
    tf.setReader(new StringReader(test));
    assertTokenStreamContents(tf,
        new String[] { "http://lucene.apache.org/java/docs/index.html#news", "here",
          "http://lucene.apache.org/java/docs/index.html?b=c", "here",
          "https://lucene.apache.org/java/docs/index.html?b=c", "here" },
        new String[] { EXTERNAL_LINK_URL, EXTERNAL_LINK,
          EXTERNAL_LINK_URL, EXTERNAL_LINK,
          EXTERNAL_LINK_URL, EXTERNAL_LINK, });
  }

  public void testLucene1133() throws Exception {
    Set<String> untoks = new HashSet<>();
    untoks.add(WikipediaTokenizer.CATEGORY);
    untoks.add(WikipediaTokenizer.ITALICS);
    //should be exactly the same, regardless of untoks
    WikipediaTokenizer tf = new WikipediaTokenizer(newAttributeFactory(), WikipediaTokenizer.TOKENS_ONLY, untoks);
    tf.setReader(new StringReader(LINK_PHRASES));
    checkLinkPhrases(tf);
    String test = "[[Category:a b c d]] [[Category:e f g]] [[link here]] [[link there]] ''italics here'' something ''more italics'' [[Category:h   i   j]]";
    tf = new WikipediaTokenizer(WikipediaTokenizer.UNTOKENIZED_ONLY, untoks);
    tf.setReader(new StringReader(test));
    assertTokenStreamContents(tf,
        new String[] { "a b c d", "e f g", "link", "here", "link",
          "there", "italics here", "something", "more italics", "h   i   j" },
        new int[] { 11, 32, 42, 47, 56, 61, 71, 86, 98, 124 },
        new int[] { 18, 37, 46, 51, 60, 66, 83, 95, 110, 133 },
        new int[] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 }
       );
  }

  public void testBoth() throws Exception {
    Set<String> untoks = new HashSet<>();
    untoks.add(WikipediaTokenizer.CATEGORY);
    untoks.add(WikipediaTokenizer.ITALICS);
    String test = "[[Category:a b c d]] [[Category:e f g]] [[link here]] [[link there]] ''italics here'' something ''more italics'' [[Category:h   i   j]]";
    //should output all the indivual tokens plus the untokenized tokens as well.  Untokenized tokens
    WikipediaTokenizer tf = new WikipediaTokenizer(newAttributeFactory(), WikipediaTokenizer.BOTH, untoks);
    tf.setReader(new StringReader(test));
    assertTokenStreamContents(tf,
        new String[] { "a b c d", "a", "b", "c", "d", "e f g", "e", "f", "g",
          "link", "here", "link", "there", "italics here", "italics", "here",
          "something", "more italics", "more", "italics", "h   i   j", "h", "i", "j" },
        new int[] { 11, 11, 13, 15, 17, 32, 32, 34, 36, 42, 47, 56, 61, 71, 71, 79, 86, 98,  98,  103, 124, 124, 128, 132 },
        new int[] { 18, 12, 14, 16, 18, 37, 33, 35, 37, 46, 51, 60, 66, 83, 78, 83, 95, 110, 102, 110, 133, 125, 129, 133 },
        new int[] { 1,  0,  1,  1,  1,  1,  0,  1,  1,  1,  1,  1,  1,  1,  0,  1,  1,  1,   0,   1,   1,   0,   1,   1 }
       );
    
    // now check the flags, TODO: add way to check flags from BaseTokenStreamTestCase?
    tf = new WikipediaTokenizer(newAttributeFactory(), WikipediaTokenizer.BOTH, untoks);
    tf.setReader(new StringReader(test));
    int expectedFlags[] = new int[] { UNTOKENIZED_TOKEN_FLAG, 0, 0, 0, 0, UNTOKENIZED_TOKEN_FLAG, 0, 0, 0, 0, 
        0, 0, 0, UNTOKENIZED_TOKEN_FLAG, 0, 0, 0, UNTOKENIZED_TOKEN_FLAG, 0, 0, UNTOKENIZED_TOKEN_FLAG, 0, 0, 0 };
    FlagsAttribute flagsAtt = tf.addAttribute(FlagsAttribute.class);
    tf.reset();
    for (int i = 0; i < expectedFlags.length; i++) {
      assertTrue(tf.incrementToken());
      assertEquals("flags " + i, expectedFlags[i], flagsAtt.getFlags());
    }
    assertFalse(tf.incrementToken());
    tf.close();
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    Analyzer a = new Analyzer() {

      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new WikipediaTokenizer(newAttributeFactory(), WikipediaTokenizer.TOKENS_ONLY, Collections.<String>emptySet());
        return new TokenStreamComponents(tokenizer, tokenizer);
      } 
    };
    // TODO: properly support positionLengthAttribute
    checkRandomData(random(), a, 1000*RANDOM_MULTIPLIER, 20, false, false);
    a.close();
  }
  
  /** blast some random large strings through the analyzer */
  public void testRandomHugeStrings() throws Exception {
    Random random = random();
    Analyzer a = new Analyzer() {

      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new WikipediaTokenizer(newAttributeFactory(), WikipediaTokenizer.TOKENS_ONLY, Collections.<String>emptySet());
        return new TokenStreamComponents(tokenizer, tokenizer);
      } 
    };
    // TODO: properly support positionLengthAttribute
    checkRandomData(random, a, 100*RANDOM_MULTIPLIER, 8192, false, false);
    a.close();
  }
}
