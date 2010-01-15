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


package org.apache.lucene.wikipedia.analysis;

import java.io.StringReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.tokenattributes.FlagsAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;


/**
 *
 *
 **/
public class WikipediaTokenizerTest extends BaseTokenStreamTestCase {
  protected static final String LINK_PHRASES = "click [[link here again]] click [http://lucene.apache.org here again] [[Category:a b c d]]";

  public WikipediaTokenizerTest(String s) {
    super(s);
  }

  public void testSimple() throws Exception {
    WikipediaTokenizer tf = new WikipediaTokenizer(new StringReader("This is a [[Category:foo]]"));
    assertTokenStreamContents(tf,
        new String[] { "This", "is", "a", "foo" });
  }
  
  public void testHandwritten() throws Exception {
    //make sure all tokens are in only one type
    String test = "[[link]] This is a [[Category:foo]] Category  This is a linked [[:Category:bar none withstanding]] " +
            "Category This is (parens) This is a [[link]]  This is an external URL [http://lucene.apache.org] " +
            "Here is ''italics'' and ''more italics'', '''bold''' and '''''five quotes''''' " +
            " This is a [[link|display info]]  This is a period.  Here is $3.25 and here is 3.50.  Here's Johnny.  " +
            "==heading== ===sub head=== followed by some text  [[Category:blah| ]] " +
            "''[[Category:ital_cat]]''  here is some that is ''italics [[Category:foo]] but is never closed." +
            "'''same [[Category:foo]] goes for this '''''and2 [[Category:foo]] and this" +
            " [http://foo.boo.com/test/test/ Test Test] [http://foo.boo.com/test/test/test.html Test Test]" +
            " [http://foo.boo.com/test/test/test.html?g=b&c=d Test Test] <ref>Citation</ref> <sup>martian</sup> <span class=\"glue\">code</span>";
    Map tcm = new HashMap();//map tokens to types
    tcm.put("link", WikipediaTokenizer.INTERNAL_LINK);
    tcm.put("display", WikipediaTokenizer.INTERNAL_LINK);
    tcm.put("info", WikipediaTokenizer.INTERNAL_LINK);

    tcm.put("http://lucene.apache.org", WikipediaTokenizer.EXTERNAL_LINK_URL);
    tcm.put("http://foo.boo.com/test/test/", WikipediaTokenizer.EXTERNAL_LINK_URL);
    tcm.put("http://foo.boo.com/test/test/test.html", WikipediaTokenizer.EXTERNAL_LINK_URL);
    tcm.put("http://foo.boo.com/test/test/test.html?g=b&c=d", WikipediaTokenizer.EXTERNAL_LINK_URL);
    tcm.put("Test", WikipediaTokenizer.EXTERNAL_LINK);
    
    //alphanums
    tcm.put("This", "<ALPHANUM>");
    tcm.put("is", "<ALPHANUM>");
    tcm.put("a", "<ALPHANUM>");
    tcm.put("Category", "<ALPHANUM>");
    tcm.put("linked", "<ALPHANUM>");
    tcm.put("parens", "<ALPHANUM>");
    tcm.put("external", "<ALPHANUM>");
    tcm.put("URL", "<ALPHANUM>");
    tcm.put("and", "<ALPHANUM>");
    tcm.put("period", "<ALPHANUM>");
    tcm.put("Here", "<ALPHANUM>");
    tcm.put("Here's", "<APOSTROPHE>");
    tcm.put("here", "<ALPHANUM>");
    tcm.put("Johnny", "<ALPHANUM>");
    tcm.put("followed", "<ALPHANUM>");
    tcm.put("by", "<ALPHANUM>");
    tcm.put("text", "<ALPHANUM>");
    tcm.put("that", "<ALPHANUM>");
    tcm.put("but", "<ALPHANUM>");
    tcm.put("never", "<ALPHANUM>");
    tcm.put("closed", "<ALPHANUM>");
    tcm.put("goes", "<ALPHANUM>");
    tcm.put("for", "<ALPHANUM>");
    tcm.put("this", "<ALPHANUM>");
    tcm.put("an", "<ALPHANUM>");
    tcm.put("some", "<ALPHANUM>");
    tcm.put("martian", "<ALPHANUM>");
    tcm.put("code", "<ALPHANUM>");

    tcm.put("foo", WikipediaTokenizer.CATEGORY);
    tcm.put("bar", WikipediaTokenizer.CATEGORY);
    tcm.put("none", WikipediaTokenizer.CATEGORY);
    tcm.put("withstanding", WikipediaTokenizer.CATEGORY);
    tcm.put("blah", WikipediaTokenizer.CATEGORY);
    tcm.put("ital", WikipediaTokenizer.CATEGORY);
    tcm.put("cat", WikipediaTokenizer.CATEGORY);

    tcm.put("italics", WikipediaTokenizer.ITALICS);
    tcm.put("more", WikipediaTokenizer.ITALICS);
    tcm.put("bold", WikipediaTokenizer.BOLD);
    tcm.put("same", WikipediaTokenizer.BOLD);
    tcm.put("five", WikipediaTokenizer.BOLD_ITALICS);
    tcm.put("and2", WikipediaTokenizer.BOLD_ITALICS);
    tcm.put("quotes", WikipediaTokenizer.BOLD_ITALICS);

    tcm.put("heading", WikipediaTokenizer.HEADING);
    tcm.put("sub", WikipediaTokenizer.SUB_HEADING);
    tcm.put("head", WikipediaTokenizer.SUB_HEADING);
    
    tcm.put("Citation", WikipediaTokenizer.CITATION);

    tcm.put("3.25", "<NUM>");
    tcm.put("3.50", "<NUM>");
    WikipediaTokenizer tf = new WikipediaTokenizer(new StringReader(test));
    int count = 0;
    int numItalics = 0;
    int numBoldItalics = 0;
    int numCategory = 0;
    int numCitation = 0;
    TermAttribute termAtt = tf.addAttribute(TermAttribute.class);
    TypeAttribute typeAtt = tf.addAttribute(TypeAttribute.class);
    
    while (tf.incrementToken()) {
      String tokText = termAtt.term();
      //System.out.println("Text: " + tokText + " Type: " + token.type());
      String expectedType = (String) tcm.get(tokText);
      assertTrue("expectedType is null and it shouldn't be for: " + tf.toString(), expectedType != null);
      assertTrue(typeAtt.type() + " is not equal to " + expectedType + " for " + tf.toString(), typeAtt.type().equals(expectedType) == true);
      count++;
      if (typeAtt.type().equals(WikipediaTokenizer.ITALICS)  == true){
        numItalics++;
      } else if (typeAtt.type().equals(WikipediaTokenizer.BOLD_ITALICS)  == true){
        numBoldItalics++;
      } else if (typeAtt.type().equals(WikipediaTokenizer.CATEGORY)  == true){
        numCategory++;
      }
      else if (typeAtt.type().equals(WikipediaTokenizer.CITATION)  == true){
        numCitation++;
      }
    }
    assertTrue("We have not seen enough tokens: " + count + " is not >= " + tcm.size(), count >= tcm.size());
    assertTrue(numItalics + " does not equal: " + 4 + " for numItalics", numItalics == 4);
    assertTrue(numBoldItalics + " does not equal: " + 3 + " for numBoldItalics", numBoldItalics == 3);
    assertTrue(numCategory + " does not equal: " + 10 + " for numCategory", numCategory == 10);
    assertTrue(numCitation + " does not equal: " + 1 + " for numCitation", numCitation == 1);
  }

  public void testLinkPhrases() throws Exception {

    WikipediaTokenizer tf = new WikipediaTokenizer(new StringReader(LINK_PHRASES));
    checkLinkPhrases(tf);
    
  }

  private void checkLinkPhrases(WikipediaTokenizer tf) throws IOException {
    TermAttribute termAtt = tf.addAttribute(TermAttribute.class);
    PositionIncrementAttribute posIncrAtt = tf.addAttribute(PositionIncrementAttribute.class);
    
    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "click", termAtt.term().equals("click") == true);
    assertTrue(posIncrAtt.getPositionIncrement() + " does not equal: " + 1, posIncrAtt.getPositionIncrement() == 1);
    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "link", termAtt.term().equals("link") == true);
    assertTrue(posIncrAtt.getPositionIncrement() + " does not equal: " + 1, posIncrAtt.getPositionIncrement() == 1);
    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "here",
        termAtt.term().equals("here") == true);
    //The link, and here should be at the same position for phrases to work
    assertTrue(posIncrAtt.getPositionIncrement() + " does not equal: " + 1, posIncrAtt.getPositionIncrement() == 1);
    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "again",
        termAtt.term().equals("again") == true);
    assertTrue(posIncrAtt.getPositionIncrement() + " does not equal: " + 1, posIncrAtt.getPositionIncrement() == 1);

    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "click",
        termAtt.term().equals("click") == true);
    assertTrue(posIncrAtt.getPositionIncrement() + " does not equal: " + 1, posIncrAtt.getPositionIncrement() == 1);

    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "http://lucene.apache.org",
        termAtt.term().equals("http://lucene.apache.org") == true);
    assertTrue(posIncrAtt.getPositionIncrement() + " does not equal: " + 1, posIncrAtt.getPositionIncrement() == 1);

    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "here",
        termAtt.term().equals("here") == true);
    assertTrue(posIncrAtt.getPositionIncrement() + " does not equal: " + 0, posIncrAtt.getPositionIncrement() == 0);

    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "again",
        termAtt.term().equals("again") == true);
    assertTrue(posIncrAtt.getPositionIncrement() + " does not equal: " + 1, posIncrAtt.getPositionIncrement() == 1);

    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "a",
        termAtt.term().equals("a") == true);
    assertTrue(posIncrAtt.getPositionIncrement() + " does not equal: " + 1, posIncrAtt.getPositionIncrement() == 1);

    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "b",
        termAtt.term().equals("b") == true);
    assertTrue(posIncrAtt.getPositionIncrement() + " does not equal: " + 1, posIncrAtt.getPositionIncrement() == 1);

    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "c",
        termAtt.term().equals("c") == true);
    assertTrue(posIncrAtt.getPositionIncrement() + " does not equal: " + 1, posIncrAtt.getPositionIncrement() == 1);

    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "d",
        termAtt.term().equals("d") == true);
    assertTrue(posIncrAtt.getPositionIncrement() + " does not equal: " + 1, posIncrAtt.getPositionIncrement() == 1);

    assertFalse(tf.incrementToken());  
  }

  public void testLinks() throws Exception {
    String test = "[http://lucene.apache.org/java/docs/index.html#news here] [http://lucene.apache.org/java/docs/index.html?b=c here] [https://lucene.apache.org/java/docs/index.html?b=c here]";
    WikipediaTokenizer tf = new WikipediaTokenizer(new StringReader(test));
    TermAttribute termAtt = tf.addAttribute(TermAttribute.class);
    TypeAttribute typeAtt = tf.addAttribute(TypeAttribute.class);
    
    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "http://lucene.apache.org/java/docs/index.html#news",
        termAtt.term().equals("http://lucene.apache.org/java/docs/index.html#news") == true);
    assertTrue(typeAtt.type() + " is not equal to " + WikipediaTokenizer.EXTERNAL_LINK_URL, typeAtt.type().equals(WikipediaTokenizer.EXTERNAL_LINK_URL) == true);
    tf.incrementToken();//skip here
    
    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "http://lucene.apache.org/java/docs/index.html?b=c",
        termAtt.term().equals("http://lucene.apache.org/java/docs/index.html?b=c") == true);
    assertTrue(typeAtt.type() + " is not equal to " + WikipediaTokenizer.EXTERNAL_LINK_URL, typeAtt.type().equals(WikipediaTokenizer.EXTERNAL_LINK_URL) == true);
    tf.incrementToken();//skip here
    
    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "https://lucene.apache.org/java/docs/index.html?b=c",
        termAtt.term().equals("https://lucene.apache.org/java/docs/index.html?b=c") == true);
    assertTrue(typeAtt.type() + " is not equal to " + WikipediaTokenizer.EXTERNAL_LINK_URL, typeAtt.type().equals(WikipediaTokenizer.EXTERNAL_LINK_URL) == true);
    
    assertTrue(tf.incrementToken());
    assertFalse(tf.incrementToken());
  }

  public void testLucene1133() throws Exception {
    Set untoks = new HashSet();
    untoks.add(WikipediaTokenizer.CATEGORY);
    untoks.add(WikipediaTokenizer.ITALICS);
    //should be exactly the same, regardless of untoks
    WikipediaTokenizer tf = new WikipediaTokenizer(new StringReader(LINK_PHRASES), WikipediaTokenizer.TOKENS_ONLY, untoks);
    checkLinkPhrases(tf);
    String test = "[[Category:a b c d]] [[Category:e f g]] [[link here]] [[link there]] ''italics here'' something ''more italics'' [[Category:h   i   j]]";
    tf = new WikipediaTokenizer(new StringReader(test), WikipediaTokenizer.UNTOKENIZED_ONLY, untoks);
    TermAttribute termAtt = tf.addAttribute(TermAttribute.class);
    PositionIncrementAttribute posIncrAtt = tf.addAttribute(PositionIncrementAttribute.class);
    OffsetAttribute offsetAtt = tf.addAttribute(OffsetAttribute.class);
    
    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "a b c d",
        termAtt.term().equals("a b c d") == true);
    assertTrue(posIncrAtt.getPositionIncrement() + " does not equal: " + 1, posIncrAtt.getPositionIncrement() == 1);
    assertTrue(offsetAtt.startOffset() + " does not equal: " + 11, offsetAtt.startOffset() == 11);
    assertTrue(offsetAtt.endOffset() + " does not equal: " + 18, offsetAtt.endOffset() == 18);
    
    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "e f g",
        termAtt.term().equals("e f g") == true);
    assertTrue(offsetAtt.startOffset() + " does not equal: " + 32, offsetAtt.startOffset() == 32);
    assertTrue(offsetAtt.endOffset() + " does not equal: " + 37, offsetAtt.endOffset() == 37);
    
    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "link",
        termAtt.term().equals("link") == true);
    assertTrue(offsetAtt.startOffset() + " does not equal: " + 42, offsetAtt.startOffset() == 42);
    assertTrue(offsetAtt.endOffset() + " does not equal: " + 46, offsetAtt.endOffset() == 46);
    
    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "here",
        termAtt.term().equals("here") == true);
    assertTrue(offsetAtt.startOffset() + " does not equal: " + 47, offsetAtt.startOffset() == 47);
    assertTrue(offsetAtt.endOffset() + " does not equal: " + 51, offsetAtt.endOffset() == 51);

    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "link",
        termAtt.term().equals("link") == true);
    assertTrue(offsetAtt.startOffset() + " does not equal: " + 56, offsetAtt.startOffset() == 56);
    assertTrue(offsetAtt.endOffset() + " does not equal: " + 60, offsetAtt.endOffset() == 60);
    
    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "there",
        termAtt.term().equals("there") == true);

    assertTrue(offsetAtt.startOffset() + " does not equal: " + 61, offsetAtt.startOffset() == 61);
    assertTrue(offsetAtt.endOffset() + " does not equal: " + 66, offsetAtt.endOffset() == 66);
    
    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "italics here",
        termAtt.term().equals("italics here") == true);
    assertTrue(offsetAtt.startOffset() + " does not equal: " + 71, offsetAtt.startOffset() == 71);
    assertTrue(offsetAtt.endOffset() + " does not equal: " + 83, offsetAtt.endOffset() == 83);
    
    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "something",
        termAtt.term().equals("something") == true);
    assertTrue(offsetAtt.startOffset() + " does not equal: " + 86, offsetAtt.startOffset() == 86);
    assertTrue(offsetAtt.endOffset() + " does not equal: " + 95, offsetAtt.endOffset() == 95);
    
    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "more italics",
        termAtt.term().equals("more italics") == true);
    assertTrue(offsetAtt.startOffset() + " does not equal: " + 98, offsetAtt.startOffset() == 98);
    assertTrue(offsetAtt.endOffset() + " does not equal: " + 110, offsetAtt.endOffset() == 110);

    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "h   i   j",
        termAtt.term().equals("h   i   j") == true);
    assertTrue(offsetAtt.startOffset() + " does not equal: " + 124, offsetAtt.startOffset() == 124);
    assertTrue(offsetAtt.endOffset() + " does not equal: " + 133, offsetAtt.endOffset() == 133);

    assertFalse(tf.incrementToken());
  }

  public void testBoth() throws Exception {
    Set untoks = new HashSet();
    untoks.add(WikipediaTokenizer.CATEGORY);
    untoks.add(WikipediaTokenizer.ITALICS);
    String test = "[[Category:a b c d]] [[Category:e f g]] [[link here]] [[link there]] ''italics here'' something ''more italics'' [[Category:h   i   j]]";
    //should output all the indivual tokens plus the untokenized tokens as well.  Untokenized tokens
    WikipediaTokenizer tf = new WikipediaTokenizer(new StringReader(test), WikipediaTokenizer.BOTH, untoks);
    TermAttribute termAtt = tf.addAttribute(TermAttribute.class);
    TypeAttribute typeAtt = tf.addAttribute(TypeAttribute.class);
    PositionIncrementAttribute posIncrAtt = tf.addAttribute(PositionIncrementAttribute.class);
    OffsetAttribute offsetAtt = tf.addAttribute(OffsetAttribute.class);
    FlagsAttribute flagsAtt = tf.addAttribute(FlagsAttribute.class);
    
    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "a b c d",
            termAtt.term().equals("a b c d") == true);
    assertTrue(posIncrAtt.getPositionIncrement() + " does not equal: " + 1, posIncrAtt.getPositionIncrement() == 1);
    assertTrue(typeAtt.type() + " is not equal to " + WikipediaTokenizer.CATEGORY, typeAtt.type().equals(WikipediaTokenizer.CATEGORY) == true);
    assertTrue(flagsAtt.getFlags() + " does not equal: " + WikipediaTokenizer.UNTOKENIZED_TOKEN_FLAG, flagsAtt.getFlags() == WikipediaTokenizer.UNTOKENIZED_TOKEN_FLAG);
    assertTrue(offsetAtt.startOffset() + " does not equal: " + 11, offsetAtt.startOffset() == 11);
    assertTrue(offsetAtt.endOffset() + " does not equal: " + 18, offsetAtt.endOffset() == 18);
    
    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "a",
            termAtt.term().equals("a") == true);
    assertTrue(posIncrAtt.getPositionIncrement() + " does not equal: " + 0, posIncrAtt.getPositionIncrement() == 0);
    assertTrue(typeAtt.type() + " is not equal to " + WikipediaTokenizer.CATEGORY, typeAtt.type().equals(WikipediaTokenizer.CATEGORY) == true);
    assertTrue(flagsAtt.getFlags() + " equals: " + WikipediaTokenizer.UNTOKENIZED_TOKEN_FLAG + " and it shouldn't", flagsAtt.getFlags() != WikipediaTokenizer.UNTOKENIZED_TOKEN_FLAG);
    assertTrue(offsetAtt.startOffset() + " does not equal: " + 11, offsetAtt.startOffset() == 11);
    assertTrue(offsetAtt.endOffset() + " does not equal: " + 12, offsetAtt.endOffset() == 12);

    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "b",
            termAtt.term().equals("b") == true);
    assertTrue(posIncrAtt.getPositionIncrement() + " does not equal: " + 1, posIncrAtt.getPositionIncrement() == 1);
    assertTrue(typeAtt.type() + " is not equal to " + WikipediaTokenizer.CATEGORY, typeAtt.type().equals(WikipediaTokenizer.CATEGORY) == true);
    assertTrue(offsetAtt.startOffset() + " does not equal: " + 13, offsetAtt.startOffset() == 13);
    assertTrue(offsetAtt.endOffset() + " does not equal: " + 14, offsetAtt.endOffset() == 14);

    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "c",
            termAtt.term().equals("c") == true);
    assertTrue(posIncrAtt.getPositionIncrement() + " does not equal: " + 1, posIncrAtt.getPositionIncrement() == 1);
    assertTrue(typeAtt.type() + " is not equal to " + WikipediaTokenizer.CATEGORY, typeAtt.type().equals(WikipediaTokenizer.CATEGORY) == true);
    assertTrue(offsetAtt.startOffset() + " does not equal: " + 15, offsetAtt.startOffset() == 15);
    assertTrue(offsetAtt.endOffset() + " does not equal: " + 16, offsetAtt.endOffset() == 16);

    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "d",
            termAtt.term().equals("d") == true);
    assertTrue(posIncrAtt.getPositionIncrement() + " does not equal: " + 1, posIncrAtt.getPositionIncrement() == 1);
    assertTrue(typeAtt.type() + " is not equal to " + WikipediaTokenizer.CATEGORY, typeAtt.type().equals(WikipediaTokenizer.CATEGORY) == true);
    assertTrue(offsetAtt.startOffset() + " does not equal: " + 17, offsetAtt.startOffset() == 17);
    assertTrue(offsetAtt.endOffset() + " does not equal: " + 18, offsetAtt.endOffset() == 18);



    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "e f g",
            termAtt.term().equals("e f g") == true);
    assertTrue(typeAtt.type() + " is not equal to " + WikipediaTokenizer.CATEGORY, typeAtt.type().equals(WikipediaTokenizer.CATEGORY) == true);
    assertTrue(flagsAtt.getFlags() + " does not equal: " + WikipediaTokenizer.UNTOKENIZED_TOKEN_FLAG, flagsAtt.getFlags() == WikipediaTokenizer.UNTOKENIZED_TOKEN_FLAG);
    assertTrue(offsetAtt.startOffset() + " does not equal: " + 32, offsetAtt.startOffset() == 32);
    assertTrue(offsetAtt.endOffset() + " does not equal: " + 37, offsetAtt.endOffset() == 37);

    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "e",
            termAtt.term().equals("e") == true);
    assertTrue(typeAtt.type() + " is not equal to " + WikipediaTokenizer.CATEGORY, typeAtt.type().equals(WikipediaTokenizer.CATEGORY) == true);
    assertTrue(posIncrAtt.getPositionIncrement() + " does not equal: " + 0, posIncrAtt.getPositionIncrement() == 0);
    assertTrue(offsetAtt.startOffset() + " does not equal: " + 32, offsetAtt.startOffset() == 32);
    assertTrue(offsetAtt.endOffset() + " does not equal: " + 33, offsetAtt.endOffset() == 33);

    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "f",
            termAtt.term().equals("f") == true);
    assertTrue(typeAtt.type() + " is not equal to " + WikipediaTokenizer.CATEGORY, typeAtt.type().equals(WikipediaTokenizer.CATEGORY) == true);
    assertTrue(posIncrAtt.getPositionIncrement() + " does not equal: " + 1, posIncrAtt.getPositionIncrement() == 1);
    assertTrue(offsetAtt.startOffset() + " does not equal: " + 34, offsetAtt.startOffset() == 34);
    assertTrue(offsetAtt.endOffset() + " does not equal: " + 35, offsetAtt.endOffset() == 35);

    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "g",
            termAtt.term().equals("g") == true);
    assertTrue(typeAtt.type() + " is not equal to " + WikipediaTokenizer.CATEGORY, typeAtt.type().equals(WikipediaTokenizer.CATEGORY) == true);
    assertTrue(posIncrAtt.getPositionIncrement() + " does not equal: " + 1, posIncrAtt.getPositionIncrement() == 1);
    assertTrue(offsetAtt.startOffset() + " does not equal: " + 36, offsetAtt.startOffset() == 36);
    assertTrue(offsetAtt.endOffset() + " does not equal: " + 37, offsetAtt.endOffset() == 37);

    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "link",
            termAtt.term().equals("link") == true);
    assertTrue(posIncrAtt.getPositionIncrement() + " does not equal: " + 1, posIncrAtt.getPositionIncrement() == 1);
    assertTrue(typeAtt.type() + " is not equal to " + WikipediaTokenizer.INTERNAL_LINK, typeAtt.type().equals(WikipediaTokenizer.INTERNAL_LINK) == true);
    assertTrue(offsetAtt.startOffset() + " does not equal: " + 42, offsetAtt.startOffset() == 42);
    assertTrue(offsetAtt.endOffset() + " does not equal: " + 46, offsetAtt.endOffset() == 46);
    
    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "here",
            termAtt.term().equals("here") == true);
    assertTrue(posIncrAtt.getPositionIncrement() + " does not equal: " + 1, posIncrAtt.getPositionIncrement() == 1);
    assertTrue(typeAtt.type() + " is not equal to " + WikipediaTokenizer.INTERNAL_LINK, typeAtt.type().equals(WikipediaTokenizer.INTERNAL_LINK) == true);
    assertTrue(offsetAtt.startOffset() + " does not equal: " + 47, offsetAtt.startOffset() == 47);
    assertTrue(offsetAtt.endOffset() + " does not equal: " + 51, offsetAtt.endOffset() == 51);
    
    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "link",
            termAtt.term().equals("link") == true);
    assertTrue(posIncrAtt.getPositionIncrement() + " does not equal: " + 1, posIncrAtt.getPositionIncrement() == 1);
    assertTrue(offsetAtt.startOffset() + " does not equal: " + 56, offsetAtt.startOffset() == 56);
    assertTrue(typeAtt.type() + " is not equal to " + WikipediaTokenizer.INTERNAL_LINK, typeAtt.type().equals(WikipediaTokenizer.INTERNAL_LINK) == true);
    assertTrue(offsetAtt.endOffset() + " does not equal: " + 60, offsetAtt.endOffset() == 60);
    
    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "there",
            termAtt.term().equals("there") == true);
    assertTrue(posIncrAtt.getPositionIncrement() + " does not equal: " + 1, posIncrAtt.getPositionIncrement() == 1);
    assertTrue(typeAtt.type() + " is not equal to " + WikipediaTokenizer.INTERNAL_LINK, typeAtt.type().equals(WikipediaTokenizer.INTERNAL_LINK) == true);
    assertTrue(offsetAtt.startOffset() + " does not equal: " + 61, offsetAtt.startOffset() == 61);
    assertTrue(offsetAtt.endOffset() + " does not equal: " + 66, offsetAtt.endOffset() == 66);
    
    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "italics here",
            termAtt.term().equals("italics here") == true);
    assertTrue(posIncrAtt.getPositionIncrement() + " does not equal: " + 1, posIncrAtt.getPositionIncrement() == 1);
    assertTrue(typeAtt.type() + " is not equal to " + WikipediaTokenizer.ITALICS, typeAtt.type().equals(WikipediaTokenizer.ITALICS) == true);
    assertTrue(flagsAtt.getFlags() + " does not equal: " + WikipediaTokenizer.UNTOKENIZED_TOKEN_FLAG, flagsAtt.getFlags() == WikipediaTokenizer.UNTOKENIZED_TOKEN_FLAG);
    assertTrue(offsetAtt.startOffset() + " does not equal: " + 71, offsetAtt.startOffset() == 71);
    assertTrue(offsetAtt.endOffset() + " does not equal: " + 83, offsetAtt.endOffset() == 83);

    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "italics",
            termAtt.term().equals("italics") == true);
    assertTrue(posIncrAtt.getPositionIncrement() + " does not equal: " + 0, posIncrAtt.getPositionIncrement() == 0);
    assertTrue(typeAtt.type() + " is not equal to " + WikipediaTokenizer.ITALICS, typeAtt.type().equals(WikipediaTokenizer.ITALICS) == true);
    assertTrue(offsetAtt.startOffset() + " does not equal: " + 71, offsetAtt.startOffset() == 71);
    assertTrue(offsetAtt.endOffset() + " does not equal: " + 78, offsetAtt.endOffset() == 78);

    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "here",
            termAtt.term().equals("here") == true);
    assertTrue(posIncrAtt.getPositionIncrement() + " does not equal: " + 1, posIncrAtt.getPositionIncrement() == 1);
    assertTrue(typeAtt.type() + " is not equal to " + WikipediaTokenizer.ITALICS, typeAtt.type().equals(WikipediaTokenizer.ITALICS) == true);
    assertTrue(offsetAtt.startOffset() + " does not equal: " + 79, offsetAtt.startOffset() == 79);
    assertTrue(offsetAtt.endOffset() + " does not equal: " + 83, offsetAtt.endOffset() == 83);

    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "something",
            termAtt.term().equals("something") == true);
    assertTrue(posIncrAtt.getPositionIncrement() + " does not equal: " + 1, posIncrAtt.getPositionIncrement() == 1);
    assertTrue(offsetAtt.startOffset() + " does not equal: " + 86, offsetAtt.startOffset() == 86);
    assertTrue(offsetAtt.endOffset() + " does not equal: " + 95, offsetAtt.endOffset() == 95);
    
    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "more italics",
            termAtt.term().equals("more italics") == true);
    assertTrue(posIncrAtt.getPositionIncrement() + " does not equal: " + 1, posIncrAtt.getPositionIncrement() == 1);
    assertTrue(typeAtt.type() + " is not equal to " + WikipediaTokenizer.ITALICS, typeAtt.type().equals(WikipediaTokenizer.ITALICS) == true);
    assertTrue(flagsAtt.getFlags() + " does not equal: " + WikipediaTokenizer.UNTOKENIZED_TOKEN_FLAG, flagsAtt.getFlags() == WikipediaTokenizer.UNTOKENIZED_TOKEN_FLAG);
    assertTrue(offsetAtt.startOffset() + " does not equal: " + 98, offsetAtt.startOffset() == 98);
    assertTrue(offsetAtt.endOffset() + " does not equal: " + 110, offsetAtt.endOffset() == 110);

    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "more",
            termAtt.term().equals("more") == true);
    assertTrue(posIncrAtt.getPositionIncrement() + " does not equal: " + 0, posIncrAtt.getPositionIncrement() == 0);
    assertTrue(typeAtt.type() + " is not equal to " + WikipediaTokenizer.ITALICS, typeAtt.type().equals(WikipediaTokenizer.ITALICS) == true);
    assertTrue(offsetAtt.startOffset() + " does not equal: " + 98, offsetAtt.startOffset() == 98);
    assertTrue(offsetAtt.endOffset() + " does not equal: " + 102, offsetAtt.endOffset() == 102);

    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "italics",
            termAtt.term().equals("italics") == true);
    assertTrue(posIncrAtt.getPositionIncrement() + " does not equal: " + 1, posIncrAtt.getPositionIncrement() == 1);
        assertTrue(typeAtt.type() + " is not equal to " + WikipediaTokenizer.ITALICS, typeAtt.type().equals(WikipediaTokenizer.ITALICS) == true);

    assertTrue(offsetAtt.startOffset() + " does not equal: " + 103, offsetAtt.startOffset() == 103);
    assertTrue(offsetAtt.endOffset() + " does not equal: " + 110, offsetAtt.endOffset() == 110);

    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "h   i   j",
            termAtt.term().equals("h   i   j") == true);
    assertTrue(posIncrAtt.getPositionIncrement() + " does not equal: " + 1, posIncrAtt.getPositionIncrement() == 1);
    assertTrue(typeAtt.type() + " is not equal to " + WikipediaTokenizer.CATEGORY, typeAtt.type().equals(WikipediaTokenizer.CATEGORY) == true);
    assertTrue(flagsAtt.getFlags() + " does not equal: " + WikipediaTokenizer.UNTOKENIZED_TOKEN_FLAG, flagsAtt.getFlags() == WikipediaTokenizer.UNTOKENIZED_TOKEN_FLAG);
    assertTrue(offsetAtt.startOffset() + " does not equal: " + 124, offsetAtt.startOffset() == 124);
    assertTrue(offsetAtt.endOffset() + " does not equal: " + 133, offsetAtt.endOffset() == 133);

    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "h",
            termAtt.term().equals("h") == true);
    assertTrue(posIncrAtt.getPositionIncrement() + " does not equal: " + 0, posIncrAtt.getPositionIncrement() == 0);
    assertTrue(typeAtt.type() + " is not equal to " + WikipediaTokenizer.CATEGORY, typeAtt.type().equals(WikipediaTokenizer.CATEGORY) == true);
    assertTrue(offsetAtt.startOffset() + " does not equal: " + 124, offsetAtt.startOffset() == 124);
    assertTrue(offsetAtt.endOffset() + " does not equal: " + 125, offsetAtt.endOffset() == 125);

    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "i",
            termAtt.term().equals("i") == true);
    assertTrue(posIncrAtt.getPositionIncrement() + " does not equal: " + 1, posIncrAtt.getPositionIncrement() == 1);
    assertTrue(typeAtt.type() + " is not equal to " + WikipediaTokenizer.CATEGORY, typeAtt.type().equals(WikipediaTokenizer.CATEGORY) == true);
    assertTrue(offsetAtt.startOffset() + " does not equal: " + 128, offsetAtt.startOffset() == 128);
    assertTrue(offsetAtt.endOffset() + " does not equal: " + 129, offsetAtt.endOffset() == 129);
    
    assertTrue(tf.incrementToken());
    assertTrue(termAtt.term() + " is not equal to " + "j",
            termAtt.term().equals("j") == true);
    assertTrue(posIncrAtt.getPositionIncrement() + " does not equal: " + 1, posIncrAtt.getPositionIncrement() == 1);
    assertTrue(typeAtt.type() + " is not equal to " + WikipediaTokenizer.CATEGORY, typeAtt.type().equals(WikipediaTokenizer.CATEGORY) == true);
    assertTrue(offsetAtt.startOffset() + " does not equal: " + 132, offsetAtt.startOffset() == 132);
    assertTrue(offsetAtt.endOffset() + " does not equal: " + 133, offsetAtt.endOffset() == 133);

    assertFalse(tf.incrementToken());
  }
}
