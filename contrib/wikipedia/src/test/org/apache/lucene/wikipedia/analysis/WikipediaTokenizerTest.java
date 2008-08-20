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

import junit.framework.TestCase;
import org.apache.lucene.analysis.Token;

import java.io.StringReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;


/**
 *
 *
 **/
public class WikipediaTokenizerTest extends TestCase {
  protected static final String LINK_PHRASES = "click [[link here again]] click [http://lucene.apache.org here again] [[Category:a b c d]]";


  public WikipediaTokenizerTest(String s) {
    super(s);
  }

  protected void setUp() {
  }

  protected void tearDown() {

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
    final Token reusableToken = new Token();
    for (Token nextToken = tf.next(reusableToken); nextToken != null; nextToken = tf.next(reusableToken)) {
      String tokText = nextToken.term();
      //System.out.println("Text: " + tokText + " Type: " + token.type());
      assertTrue("nextToken is null and it shouldn't be", nextToken != null);
      String expectedType = (String) tcm.get(tokText);
      assertTrue("expectedType is null and it shouldn't be for: " + nextToken, expectedType != null);
      assertTrue(nextToken.type() + " is not equal to " + expectedType + " for " + nextToken, nextToken.type().equals(expectedType) == true);
      count++;
      if (nextToken.type().equals(WikipediaTokenizer.ITALICS)  == true){
        numItalics++;
      } else if (nextToken.type().equals(WikipediaTokenizer.BOLD_ITALICS)  == true){
        numBoldItalics++;
      } else if (nextToken.type().equals(WikipediaTokenizer.CATEGORY)  == true){
        numCategory++;
      }
      else if (nextToken.type().equals(WikipediaTokenizer.CITATION)  == true){
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
    final Token reusableToken = new Token();
    Token nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "click", nextToken.term().equals("click") == true);
    assertTrue(nextToken.getPositionIncrement() + " does not equal: " + 1, nextToken.getPositionIncrement() == 1);
    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "link", nextToken.term().equals("link") == true);
    assertTrue(nextToken.getPositionIncrement() + " does not equal: " + 1, nextToken.getPositionIncrement() == 1);
    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "here",
            nextToken.term().equals("here") == true);
    //The link, and here should be at the same position for phrases to work
    assertTrue(nextToken.getPositionIncrement() + " does not equal: " + 1, nextToken.getPositionIncrement() == 1);
    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "again",
            nextToken.term().equals("again") == true);
    assertTrue(nextToken.getPositionIncrement() + " does not equal: " + 1, nextToken.getPositionIncrement() == 1);

    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "click",
            nextToken.term().equals("click") == true);
    assertTrue(nextToken.getPositionIncrement() + " does not equal: " + 1, nextToken.getPositionIncrement() == 1);

    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "http://lucene.apache.org",
            nextToken.term().equals("http://lucene.apache.org") == true);
    assertTrue(nextToken.getPositionIncrement() + " does not equal: " + 1, nextToken.getPositionIncrement() == 1);

    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "here",
            nextToken.term().equals("here") == true);
    assertTrue(nextToken.getPositionIncrement() + " does not equal: " + 0, nextToken.getPositionIncrement() == 0);

    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "again",
            nextToken.term().equals("again") == true);
    assertTrue(nextToken.getPositionIncrement() + " does not equal: " + 1, nextToken.getPositionIncrement() == 1);

    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "a",
            nextToken.term().equals("a") == true);
    assertTrue(nextToken.getPositionIncrement() + " does not equal: " + 1, nextToken.getPositionIncrement() == 1);

    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "b",
            nextToken.term().equals("b") == true);
    assertTrue(nextToken.getPositionIncrement() + " does not equal: " + 1, nextToken.getPositionIncrement() == 1);

    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "c",
            nextToken.term().equals("c") == true);
    assertTrue(nextToken.getPositionIncrement() + " does not equal: " + 1, nextToken.getPositionIncrement() == 1);

    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "d",
            nextToken.term().equals("d") == true);
    assertTrue(nextToken.getPositionIncrement() + " does not equal: " + 1, nextToken.getPositionIncrement() == 1);

    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is not null and it should be", nextToken == null);
  }

  public void testLinks() throws Exception {
    String test = "[http://lucene.apache.org/java/docs/index.html#news here] [http://lucene.apache.org/java/docs/index.html?b=c here] [https://lucene.apache.org/java/docs/index.html?b=c here]";
    WikipediaTokenizer tf = new WikipediaTokenizer(new StringReader(test));
    final Token reusableToken = new Token();
    Token nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "http://lucene.apache.org/java/docs/index.html#news",
            nextToken.term().equals("http://lucene.apache.org/java/docs/index.html#news") == true);
    assertTrue(nextToken.type() + " is not equal to " + WikipediaTokenizer.EXTERNAL_LINK_URL, nextToken.type().equals(WikipediaTokenizer.EXTERNAL_LINK_URL) == true);
    tf.next(reusableToken);//skip here
    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "http://lucene.apache.org/java/docs/index.html?b=c",
            nextToken.term().equals("http://lucene.apache.org/java/docs/index.html?b=c") == true);
    assertTrue(nextToken.type() + " is not equal to " + WikipediaTokenizer.EXTERNAL_LINK_URL, nextToken.type().equals(WikipediaTokenizer.EXTERNAL_LINK_URL) == true);
    tf.next(reusableToken);//skip here
    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "https://lucene.apache.org/java/docs/index.html?b=c",
            nextToken.term().equals("https://lucene.apache.org/java/docs/index.html?b=c") == true);
    assertTrue(nextToken.type() + " is not equal to " + WikipediaTokenizer.EXTERNAL_LINK_URL, nextToken.type().equals(WikipediaTokenizer.EXTERNAL_LINK_URL) == true);
    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);

    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is not null and it should be", nextToken == null);

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
    final Token reusableToken = new Token();
    Token nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "a b c d",
            nextToken.term().equals("a b c d") == true);
    assertTrue(nextToken.getPositionIncrement() + " does not equal: " + 1, nextToken.getPositionIncrement() == 1);
    assertTrue(nextToken.startOffset() + " does not equal: " + 11, nextToken.startOffset() == 11);
    assertTrue(nextToken.endOffset() + " does not equal: " + 18, nextToken.endOffset() == 18);
    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "e f g",
            nextToken.term().equals("e f g") == true);
    assertTrue(nextToken.startOffset() + " does not equal: " + 32, nextToken.startOffset() == 32);
    assertTrue(nextToken.endOffset() + " does not equal: " + 37, nextToken.endOffset() == 37);
    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "link",
            nextToken.term().equals("link") == true);
    assertTrue(nextToken.startOffset() + " does not equal: " + 42, nextToken.startOffset() == 42);
    assertTrue(nextToken.endOffset() + " does not equal: " + 46, nextToken.endOffset() == 46);
    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "here",
            nextToken.term().equals("here") == true);
    assertTrue(nextToken.startOffset() + " does not equal: " + 47, nextToken.startOffset() == 47);
    assertTrue(nextToken.endOffset() + " does not equal: " + 51, nextToken.endOffset() == 51);
    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "link",
            nextToken.term().equals("link") == true);
    assertTrue(nextToken.startOffset() + " does not equal: " + 56, nextToken.startOffset() == 56);
    assertTrue(nextToken.endOffset() + " does not equal: " + 60, nextToken.endOffset() == 60);
    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "there",
            nextToken.term().equals("there") == true);
    assertTrue(nextToken.startOffset() + " does not equal: " + 61, nextToken.startOffset() == 61);
    assertTrue(nextToken.endOffset() + " does not equal: " + 66, nextToken.endOffset() == 66);
    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "italics here",
            nextToken.term().equals("italics here") == true);
    assertTrue(nextToken.startOffset() + " does not equal: " + 71, nextToken.startOffset() == 71);
    assertTrue(nextToken.endOffset() + " does not equal: " + 83, nextToken.endOffset() == 83);
    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "something",
            nextToken.term().equals("something") == true);
    assertTrue(nextToken.startOffset() + " does not equal: " + 86, nextToken.startOffset() == 86);
    assertTrue(nextToken.endOffset() + " does not equal: " + 95, nextToken.endOffset() == 95);
    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "more italics",
            nextToken.term().equals("more italics") == true);
    assertTrue(nextToken.startOffset() + " does not equal: " + 98, nextToken.startOffset() == 98);
    assertTrue(nextToken.endOffset() + " does not equal: " + 110, nextToken.endOffset() == 110);

    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "h   i   j",
            nextToken.term().equals("h   i   j") == true);
    assertTrue(nextToken.startOffset() + " does not equal: " + 124, nextToken.startOffset() == 124);
    assertTrue(nextToken.endOffset() + " does not equal: " + 133, nextToken.endOffset() == 133);

    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is not null and it should be", nextToken == null);
  }

  public void testBoth() throws Exception {
    Set untoks = new HashSet();
    untoks.add(WikipediaTokenizer.CATEGORY);
    untoks.add(WikipediaTokenizer.ITALICS);
    String test = "[[Category:a b c d]] [[Category:e f g]] [[link here]] [[link there]] ''italics here'' something ''more italics'' [[Category:h   i   j]]";
    //should output all the indivual tokens plus the untokenized tokens as well.  Untokenized tokens
    WikipediaTokenizer tf = new WikipediaTokenizer(new StringReader(test), WikipediaTokenizer.BOTH, untoks);
    final Token reusableToken = new Token();
    Token nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "a b c d",
            nextToken.term().equals("a b c d") == true);
    assertTrue(nextToken.getPositionIncrement() + " does not equal: " + 1, nextToken.getPositionIncrement() == 1);
    assertTrue(nextToken.type() + " is not equal to " + WikipediaTokenizer.CATEGORY, nextToken.type().equals(WikipediaTokenizer.CATEGORY) == true);
    assertTrue(nextToken.getFlags() + " does not equal: " + WikipediaTokenizer.UNTOKENIZED_TOKEN_FLAG, nextToken.getFlags() == WikipediaTokenizer.UNTOKENIZED_TOKEN_FLAG);
    assertTrue(nextToken.startOffset() + " does not equal: " + 11, nextToken.startOffset() == 11);
    assertTrue(nextToken.endOffset() + " does not equal: " + 18, nextToken.endOffset() == 18);
    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "a",
            nextToken.term().equals("a") == true);
    assertTrue(nextToken.getPositionIncrement() + " does not equal: " + 0, nextToken.getPositionIncrement() == 0);
    assertTrue(nextToken.type() + " is not equal to " + WikipediaTokenizer.CATEGORY, nextToken.type().equals(WikipediaTokenizer.CATEGORY) == true);
    assertTrue(nextToken.getFlags() + " equals: " + WikipediaTokenizer.UNTOKENIZED_TOKEN_FLAG + " and it shouldn't", nextToken.getFlags() != WikipediaTokenizer.UNTOKENIZED_TOKEN_FLAG);
    assertTrue(nextToken.startOffset() + " does not equal: " + 11, nextToken.startOffset() == 11);
    assertTrue(nextToken.endOffset() + " does not equal: " + 12, nextToken.endOffset() == 12);

    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "b",
            nextToken.term().equals("b") == true);
    assertTrue(nextToken.getPositionIncrement() + " does not equal: " + 1, nextToken.getPositionIncrement() == 1);
    assertTrue(nextToken.type() + " is not equal to " + WikipediaTokenizer.CATEGORY, nextToken.type().equals(WikipediaTokenizer.CATEGORY) == true);
    assertTrue(nextToken.startOffset() + " does not equal: " + 13, nextToken.startOffset() == 13);
    assertTrue(nextToken.endOffset() + " does not equal: " + 14, nextToken.endOffset() == 14);

    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "c",
            nextToken.term().equals("c") == true);
    assertTrue(nextToken.getPositionIncrement() + " does not equal: " + 1, nextToken.getPositionIncrement() == 1);
    assertTrue(nextToken.type() + " is not equal to " + WikipediaTokenizer.CATEGORY, nextToken.type().equals(WikipediaTokenizer.CATEGORY) == true);
    assertTrue(nextToken.startOffset() + " does not equal: " + 15, nextToken.startOffset() == 15);
    assertTrue(nextToken.endOffset() + " does not equal: " + 16, nextToken.endOffset() == 16);

    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "d",
            nextToken.term().equals("d") == true);
    assertTrue(nextToken.getPositionIncrement() + " does not equal: " + 1, nextToken.getPositionIncrement() == 1);
    assertTrue(nextToken.type() + " is not equal to " + WikipediaTokenizer.CATEGORY, nextToken.type().equals(WikipediaTokenizer.CATEGORY) == true);
    assertTrue(nextToken.startOffset() + " does not equal: " + 17, nextToken.startOffset() == 17);
    assertTrue(nextToken.endOffset() + " does not equal: " + 18, nextToken.endOffset() == 18);



    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "e f g",
            nextToken.term().equals("e f g") == true);
    assertTrue(nextToken.type() + " is not equal to " + WikipediaTokenizer.CATEGORY, nextToken.type().equals(WikipediaTokenizer.CATEGORY) == true);
    assertTrue(nextToken.getFlags() + " does not equal: " + WikipediaTokenizer.UNTOKENIZED_TOKEN_FLAG, nextToken.getFlags() == WikipediaTokenizer.UNTOKENIZED_TOKEN_FLAG);
    assertTrue(nextToken.startOffset() + " does not equal: " + 32, nextToken.startOffset() == 32);
    assertTrue(nextToken.endOffset() + " does not equal: " + 37, nextToken.endOffset() == 37);

    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "e",
            nextToken.term().equals("e") == true);
    assertTrue(nextToken.type() + " is not equal to " + WikipediaTokenizer.CATEGORY, nextToken.type().equals(WikipediaTokenizer.CATEGORY) == true);
    assertTrue(nextToken.getPositionIncrement() + " does not equal: " + 0, nextToken.getPositionIncrement() == 0);
    assertTrue(nextToken.startOffset() + " does not equal: " + 32, nextToken.startOffset() == 32);
    assertTrue(nextToken.endOffset() + " does not equal: " + 33, nextToken.endOffset() == 33);

    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "f",
            nextToken.term().equals("f") == true);
    assertTrue(nextToken.type() + " is not equal to " + WikipediaTokenizer.CATEGORY, nextToken.type().equals(WikipediaTokenizer.CATEGORY) == true);
    assertTrue(nextToken.getPositionIncrement() + " does not equal: " + 1, nextToken.getPositionIncrement() == 1);
    assertTrue(nextToken.startOffset() + " does not equal: " + 34, nextToken.startOffset() == 34);
    assertTrue(nextToken.endOffset() + " does not equal: " + 35, nextToken.endOffset() == 35);

    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "g",
            nextToken.term().equals("g") == true);
    assertTrue(nextToken.type() + " is not equal to " + WikipediaTokenizer.CATEGORY, nextToken.type().equals(WikipediaTokenizer.CATEGORY) == true);
    assertTrue(nextToken.getPositionIncrement() + " does not equal: " + 1, nextToken.getPositionIncrement() == 1);
    assertTrue(nextToken.startOffset() + " does not equal: " + 36, nextToken.startOffset() == 36);
    assertTrue(nextToken.endOffset() + " does not equal: " + 37, nextToken.endOffset() == 37);

    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "link",
            nextToken.term().equals("link") == true);
    assertTrue(nextToken.getPositionIncrement() + " does not equal: " + 1, nextToken.getPositionIncrement() == 1);
    assertTrue(nextToken.type() + " is not equal to " + WikipediaTokenizer.INTERNAL_LINK, nextToken.type().equals(WikipediaTokenizer.INTERNAL_LINK) == true);
    assertTrue(nextToken.startOffset() + " does not equal: " + 42, nextToken.startOffset() == 42);
    assertTrue(nextToken.endOffset() + " does not equal: " + 46, nextToken.endOffset() == 46);
    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "here",
            nextToken.term().equals("here") == true);
    assertTrue(nextToken.getPositionIncrement() + " does not equal: " + 1, nextToken.getPositionIncrement() == 1);
    assertTrue(nextToken.type() + " is not equal to " + WikipediaTokenizer.INTERNAL_LINK, nextToken.type().equals(WikipediaTokenizer.INTERNAL_LINK) == true);
    assertTrue(nextToken.startOffset() + " does not equal: " + 47, nextToken.startOffset() == 47);
    assertTrue(nextToken.endOffset() + " does not equal: " + 51, nextToken.endOffset() == 51);
    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "link",
            nextToken.term().equals("link") == true);
    assertTrue(nextToken.getPositionIncrement() + " does not equal: " + 1, nextToken.getPositionIncrement() == 1);
    assertTrue(nextToken.startOffset() + " does not equal: " + 56, nextToken.startOffset() == 56);
    assertTrue(nextToken.type() + " is not equal to " + WikipediaTokenizer.INTERNAL_LINK, nextToken.type().equals(WikipediaTokenizer.INTERNAL_LINK) == true);
    assertTrue(nextToken.endOffset() + " does not equal: " + 60, nextToken.endOffset() == 60);
    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "there",
            nextToken.term().equals("there") == true);
    assertTrue(nextToken.getPositionIncrement() + " does not equal: " + 1, nextToken.getPositionIncrement() == 1);
    assertTrue(nextToken.type() + " is not equal to " + WikipediaTokenizer.INTERNAL_LINK, nextToken.type().equals(WikipediaTokenizer.INTERNAL_LINK) == true);
    assertTrue(nextToken.startOffset() + " does not equal: " + 61, nextToken.startOffset() == 61);
    assertTrue(nextToken.endOffset() + " does not equal: " + 66, nextToken.endOffset() == 66);
    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "italics here",
            nextToken.term().equals("italics here") == true);
    assertTrue(nextToken.getPositionIncrement() + " does not equal: " + 1, nextToken.getPositionIncrement() == 1);
    assertTrue(nextToken.type() + " is not equal to " + WikipediaTokenizer.ITALICS, nextToken.type().equals(WikipediaTokenizer.ITALICS) == true);
    assertTrue(nextToken.getFlags() + " does not equal: " + WikipediaTokenizer.UNTOKENIZED_TOKEN_FLAG, nextToken.getFlags() == WikipediaTokenizer.UNTOKENIZED_TOKEN_FLAG);
    assertTrue(nextToken.startOffset() + " does not equal: " + 71, nextToken.startOffset() == 71);
    assertTrue(nextToken.endOffset() + " does not equal: " + 83, nextToken.endOffset() == 83);

    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "italics",
            nextToken.term().equals("italics") == true);
    assertTrue(nextToken.getPositionIncrement() + " does not equal: " + 0, nextToken.getPositionIncrement() == 0);
    assertTrue(nextToken.type() + " is not equal to " + WikipediaTokenizer.ITALICS, nextToken.type().equals(WikipediaTokenizer.ITALICS) == true);
    assertTrue(nextToken.startOffset() + " does not equal: " + 71, nextToken.startOffset() == 71);
    assertTrue(nextToken.endOffset() + " does not equal: " + 78, nextToken.endOffset() == 78);

    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "here",
            nextToken.term().equals("here") == true);
    assertTrue(nextToken.getPositionIncrement() + " does not equal: " + 1, nextToken.getPositionIncrement() == 1);
    assertTrue(nextToken.type() + " is not equal to " + WikipediaTokenizer.ITALICS, nextToken.type().equals(WikipediaTokenizer.ITALICS) == true);
    assertTrue(nextToken.startOffset() + " does not equal: " + 79, nextToken.startOffset() == 79);
    assertTrue(nextToken.endOffset() + " does not equal: " + 83, nextToken.endOffset() == 83);

    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "something",
            nextToken.term().equals("something") == true);
    assertTrue(nextToken.getPositionIncrement() + " does not equal: " + 1, nextToken.getPositionIncrement() == 1);
    assertTrue(nextToken.startOffset() + " does not equal: " + 86, nextToken.startOffset() == 86);
    assertTrue(nextToken.endOffset() + " does not equal: " + 95, nextToken.endOffset() == 95);
    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "more italics",
            nextToken.term().equals("more italics") == true);
    assertTrue(nextToken.getPositionIncrement() + " does not equal: " + 1, nextToken.getPositionIncrement() == 1);
    assertTrue(nextToken.type() + " is not equal to " + WikipediaTokenizer.ITALICS, nextToken.type().equals(WikipediaTokenizer.ITALICS) == true);
    assertTrue(nextToken.getFlags() + " does not equal: " + WikipediaTokenizer.UNTOKENIZED_TOKEN_FLAG, nextToken.getFlags() == WikipediaTokenizer.UNTOKENIZED_TOKEN_FLAG);
    assertTrue(nextToken.startOffset() + " does not equal: " + 98, nextToken.startOffset() == 98);
    assertTrue(nextToken.endOffset() + " does not equal: " + 110, nextToken.endOffset() == 110);

    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "more",
            nextToken.term().equals("more") == true);
    assertTrue(nextToken.getPositionIncrement() + " does not equal: " + 0, nextToken.getPositionIncrement() == 0);
    assertTrue(nextToken.type() + " is not equal to " + WikipediaTokenizer.ITALICS, nextToken.type().equals(WikipediaTokenizer.ITALICS) == true);
    assertTrue(nextToken.startOffset() + " does not equal: " + 98, nextToken.startOffset() == 98);
    assertTrue(nextToken.endOffset() + " does not equal: " + 102, nextToken.endOffset() == 102);

    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "italics",
            nextToken.term().equals("italics") == true);
    assertTrue(nextToken.getPositionIncrement() + " does not equal: " + 1, nextToken.getPositionIncrement() == 1);
        assertTrue(nextToken.type() + " is not equal to " + WikipediaTokenizer.ITALICS, nextToken.type().equals(WikipediaTokenizer.ITALICS) == true);

    assertTrue(nextToken.startOffset() + " does not equal: " + 103, nextToken.startOffset() == 103);
    assertTrue(nextToken.endOffset() + " does not equal: " + 110, nextToken.endOffset() == 110);

    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "h   i   j",
            nextToken.term().equals("h   i   j") == true);
    assertTrue(nextToken.getPositionIncrement() + " does not equal: " + 1, nextToken.getPositionIncrement() == 1);
    assertTrue(nextToken.type() + " is not equal to " + WikipediaTokenizer.CATEGORY, nextToken.type().equals(WikipediaTokenizer.CATEGORY) == true);
    assertTrue(nextToken.getFlags() + " does not equal: " + WikipediaTokenizer.UNTOKENIZED_TOKEN_FLAG, nextToken.getFlags() == WikipediaTokenizer.UNTOKENIZED_TOKEN_FLAG);
    assertTrue(nextToken.startOffset() + " does not equal: " + 124, nextToken.startOffset() == 124);
    assertTrue(nextToken.endOffset() + " does not equal: " + 133, nextToken.endOffset() == 133);

    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "h",
            nextToken.term().equals("h") == true);
    assertTrue(nextToken.getPositionIncrement() + " does not equal: " + 0, nextToken.getPositionIncrement() == 0);
    assertTrue(nextToken.type() + " is not equal to " + WikipediaTokenizer.CATEGORY, nextToken.type().equals(WikipediaTokenizer.CATEGORY) == true);
    assertTrue(nextToken.startOffset() + " does not equal: " + 124, nextToken.startOffset() == 124);
    assertTrue(nextToken.endOffset() + " does not equal: " + 125, nextToken.endOffset() == 125);

    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "i",
            nextToken.term().equals("i") == true);
    assertTrue(nextToken.getPositionIncrement() + " does not equal: " + 1, nextToken.getPositionIncrement() == 1);
    assertTrue(nextToken.type() + " is not equal to " + WikipediaTokenizer.CATEGORY, nextToken.type().equals(WikipediaTokenizer.CATEGORY) == true);
    assertTrue(nextToken.startOffset() + " does not equal: " + 128, nextToken.startOffset() == 128);
    assertTrue(nextToken.endOffset() + " does not equal: " + 129, nextToken.endOffset() == 129);
    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is null and it shouldn't be", nextToken != null);
    assertTrue(nextToken.term() + " is not equal to " + "j",
            nextToken.term().equals("j") == true);
    assertTrue(nextToken.getPositionIncrement() + " does not equal: " + 1, nextToken.getPositionIncrement() == 1);
    assertTrue(nextToken.type() + " is not equal to " + WikipediaTokenizer.CATEGORY, nextToken.type().equals(WikipediaTokenizer.CATEGORY) == true);
    assertTrue(nextToken.startOffset() + " does not equal: " + 132, nextToken.startOffset() == 132);
    assertTrue(nextToken.endOffset() + " does not equal: " + 133, nextToken.endOffset() == 133);

    nextToken = tf.next(reusableToken);
    assertTrue("nextToken is not null and it should be", nextToken == null);

  }
}
