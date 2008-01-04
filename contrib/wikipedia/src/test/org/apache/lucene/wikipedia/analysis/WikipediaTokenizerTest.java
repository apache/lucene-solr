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
import java.util.HashMap;
import java.util.Map;


/**
 *
 *
 **/
public class WikipediaTokenizerTest extends TestCase {


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
    Token token = new Token();
    int count = 0;
    int numItalics = 0;
    int numBoldItalics = 0;
    int numCategory = 0;
    int numCitation = 0;
    while ((token = tf.next(token)) != null) {
      String tokText = token.termText();
      //System.out.println("Text: " + tokText + " Type: " + token.type());
      assertTrue("token is null and it shouldn't be", token != null);
      String expectedType = (String) tcm.get(tokText);
      assertTrue("expectedType is null and it shouldn't be for: " + token, expectedType != null);
      assertTrue(token.type() + " is not equal to " + expectedType + " for " + token, token.type().equals(expectedType) == true);
      count++;
      if (token.type().equals(WikipediaTokenizer.ITALICS)  == true){
        numItalics++;
      } else if (token.type().equals(WikipediaTokenizer.BOLD_ITALICS)  == true){
        numBoldItalics++;
      } else if (token.type().equals(WikipediaTokenizer.CATEGORY)  == true){
        numCategory++;
      }
      else if (token.type().equals(WikipediaTokenizer.CITATION)  == true){
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
    String test = "click [[link here again]] click [http://lucene.apache.org here again]";
    WikipediaTokenizer tf = new WikipediaTokenizer(new StringReader(test));
    Token token = new Token();
    token = tf.next(token);
    assertTrue("token is null and it shouldn't be", token != null);
    assertTrue(new String(token.termBuffer(), 0, token.termLength()) + " is not equal to " + "click", new String(token.termBuffer(), 0, token.termLength()).equals("click") == true);
    assertTrue(token.getPositionIncrement() + " does not equal: " + 1, token.getPositionIncrement() == 1);
    token = tf.next(token);
    assertTrue("token is null and it shouldn't be", token != null);
    assertTrue(new String(token.termBuffer(), 0, token.termLength()) + " is not equal to " + "link", new String(token.termBuffer(), 0, token.termLength()).equals("link") == true);
    assertTrue(token.getPositionIncrement() + " does not equal: " + 1, token.getPositionIncrement() == 1);
    token = tf.next(token);
    assertTrue("token is null and it shouldn't be", token != null);
    assertTrue(new String(token.termBuffer(), 0, token.termLength()) + " is not equal to " + "here",
            new String(token.termBuffer(), 0, token.termLength()).equals("here") == true);
    //The link, and here should be at the same position for phrases to work
    assertTrue(token.getPositionIncrement() + " does not equal: " + 1, token.getPositionIncrement() == 1);
    token = tf.next(token);
    assertTrue("token is null and it shouldn't be", token != null);
    assertTrue(new String(token.termBuffer(), 0, token.termLength()) + " is not equal to " + "again",
            new String(token.termBuffer(), 0, token.termLength()).equals("again") == true);
    assertTrue(token.getPositionIncrement() + " does not equal: " + 1, token.getPositionIncrement() == 1);

    token = tf.next(token);
    assertTrue("token is null and it shouldn't be", token != null);
    assertTrue(new String(token.termBuffer(), 0, token.termLength()) + " is not equal to " + "click",
            new String(token.termBuffer(), 0, token.termLength()).equals("click") == true);
    assertTrue(token.getPositionIncrement() + " does not equal: " + 1, token.getPositionIncrement() == 1);

    token = tf.next(token);
    assertTrue("token is null and it shouldn't be", token != null);
    assertTrue(new String(token.termBuffer(), 0, token.termLength()) + " is not equal to " + "http://lucene.apache.org",
            new String(token.termBuffer(), 0, token.termLength()).equals("http://lucene.apache.org") == true);
    assertTrue(token.getPositionIncrement() + " does not equal: " + 1, token.getPositionIncrement() == 1);

    token = tf.next(token);
    assertTrue("token is null and it shouldn't be", token != null);
    assertTrue(new String(token.termBuffer(), 0, token.termLength()) + " is not equal to " + "here",
            new String(token.termBuffer(), 0, token.termLength()).equals("here") == true);
    assertTrue(token.getPositionIncrement() + " does not equal: " + 0, token.getPositionIncrement() == 0);

    token = tf.next(token);
    assertTrue("token is null and it shouldn't be", token != null);
    assertTrue(new String(token.termBuffer(), 0, token.termLength()) + " is not equal to " + "again",
            new String(token.termBuffer(), 0, token.termLength()).equals("again") == true);
    assertTrue(token.getPositionIncrement() + " does not equal: " + 1, token.getPositionIncrement() == 1);
    
  }

  public void testLinks() throws Exception {
    String test = "[http://lucene.apache.org/java/docs/index.html#news here] [http://lucene.apache.org/java/docs/index.html?b=c here] [https://lucene.apache.org/java/docs/index.html?b=c here]";
    WikipediaTokenizer tf = new WikipediaTokenizer(new StringReader(test));
    Token token = new Token();
    token = tf.next(token);
    assertTrue("token is null and it shouldn't be", token != null);
    assertTrue(new String(token.termBuffer(), 0, token.termLength()) + " is not equal to " + "http://lucene.apache.org/java/docs/index.html#news",
            new String(token.termBuffer(), 0, token.termLength()).equals("http://lucene.apache.org/java/docs/index.html#news") == true);
    assertTrue(token.type() + " is not equal to " + WikipediaTokenizer.EXTERNAL_LINK_URL, token.type().equals(WikipediaTokenizer.EXTERNAL_LINK_URL) == true);
    tf.next(token);//skip here
    token = tf.next(token);
    assertTrue("token is null and it shouldn't be", token != null);
    assertTrue(new String(token.termBuffer(), 0, token.termLength()) + " is not equal to " + "http://lucene.apache.org/java/docs/index.html?b=c",
            new String(token.termBuffer(), 0, token.termLength()).equals("http://lucene.apache.org/java/docs/index.html?b=c") == true);
    assertTrue(token.type() + " is not equal to " + WikipediaTokenizer.EXTERNAL_LINK_URL, token.type().equals(WikipediaTokenizer.EXTERNAL_LINK_URL) == true);
    tf.next(token);//skip here
    token = tf.next(token);
    assertTrue("token is null and it shouldn't be", token != null);
    assertTrue(new String(token.termBuffer(), 0, token.termLength()) + " is not equal to " + "https://lucene.apache.org/java/docs/index.html?b=c",
            new String(token.termBuffer(), 0, token.termLength()).equals("https://lucene.apache.org/java/docs/index.html?b=c") == true);
    assertTrue(token.type() + " is not equal to " + WikipediaTokenizer.EXTERNAL_LINK_URL, token.type().equals(WikipediaTokenizer.EXTERNAL_LINK_URL) == true);
  }
}
