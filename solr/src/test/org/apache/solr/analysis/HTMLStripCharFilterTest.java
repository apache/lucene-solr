package org.apache.solr.analysis;

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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.CharReader;
import org.apache.lucene.util.LuceneTestCase;

import org.apache.solr.SolrTestCaseJ4;

public class HTMLStripCharFilterTest extends LuceneTestCase {

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }
  //this is some text  here is a  link  and another  link . This is an entity: & plus a <.  Here is an &
  //
  public void test() throws IOException {
    String html = "<div class=\"foo\">this is some text</div> here is a <a href=\"#bar\">link</a> and " +
            "another <a href=\"http://lucene.apache.org/\">link</a>. " +
            "This is an entity: &amp; plus a &lt;.  Here is an &. <!-- is a comment -->";
    String gold = " this is some text  here is a  link  and " +
            "another  link . " +
            "This is an entity: & plus a <.  Here is an &.  ";
    HTMLStripCharFilter reader = new HTMLStripCharFilter(CharReader.get(new StringReader(html)));
    StringBuilder builder = new StringBuilder();
    int ch = -1;
    char [] goldArray = gold.toCharArray();
    int position = 0;
    while ((ch = reader.read()) != -1){
      char theChar = (char) ch;
      builder.append(theChar);
      assertTrue("\"" + theChar + "\"" + " at position: " + position + " does not equal: " + goldArray[position]
              + " Buffer so far: " + builder + "<EOB>", theChar == goldArray[position]);
      position++;
    }
    assertEquals(gold, builder.toString());
  }

  //Some sanity checks, but not a full-fledged check
  public void testHTML() throws Exception {

    HTMLStripCharFilter reader = new HTMLStripCharFilter(CharReader.get(new FileReader(SolrTestCaseJ4.getFile("htmlStripReaderTest.html"))));
    StringBuilder builder = new StringBuilder();
    int ch = -1;
    while ((ch = reader.read()) != -1){
      builder.append((char)ch);
    }
    String str = builder.toString();
    assertTrue("Entity not properly escaped", str.indexOf("&lt;") == -1);//there is one > in the text
    assertTrue("Forrest should have been stripped out", str.indexOf("forrest") == -1 && str.indexOf("Forrest") == -1);
    assertTrue("File should start with 'Welcome to Solr' after trimming", str.trim().startsWith("Welcome to Solr"));

    assertTrue("File should start with 'Foundation.' after trimming", str.trim().endsWith("Foundation."));
    
  }

  public void testGamma() throws Exception {
    String test = "&Gamma;";
    String gold = "\u0393";
    Set<String> set = new HashSet<String>();
    set.add("reserved");
    Reader reader = new HTMLStripCharFilter(CharReader.get(new StringReader(test)), set);
    StringBuilder builder = new StringBuilder();
    int ch = 0;
    while ((ch = reader.read()) != -1){
      builder.append((char)ch);
    }
    String result = builder.toString();
    // System.out.println("Resu: " + result + "<EOL>");
    // System.out.println("Gold: " + gold + "<EOL>");
    assertTrue(result + " is not equal to " + gold + "<EOS>", result.equals(gold) == true);
  }

  public void testEntities() throws Exception {
    String test = "&nbsp; &lt;foo&gt; &Uuml;bermensch &#61; &Gamma; bar &#x393;";
    String gold = "  <foo> \u00DCbermensch = \u0393 bar \u0393";
    Set<String> set = new HashSet<String>();
    set.add("reserved");
    Reader reader = new HTMLStripCharFilter(CharReader.get(new StringReader(test)), set);
    StringBuilder builder = new StringBuilder();
    int ch = 0;
    while ((ch = reader.read()) != -1){
      builder.append((char)ch);
    }
    String result = builder.toString();
    // System.out.println("Resu: " + result + "<EOL>");
    // System.out.println("Gold: " + gold + "<EOL>");
    assertTrue(result + " is not equal to " + gold + "<EOS>", result.equals(gold) == true);
  }

  public void testMoreEntities() throws Exception {
    String test = "&nbsp; &lt;junk/&gt; &nbsp; &#33; &#64; and &#8217;";
    String gold = "  <junk/>   ! @ and ’";
    Set<String> set = new HashSet<String>();
    set.add("reserved");
    Reader reader = new HTMLStripCharFilter(CharReader.get(new StringReader(test)), set);
    StringBuilder builder = new StringBuilder();
    int ch = 0;
    while ((ch = reader.read()) != -1){
      builder.append((char)ch);
    }
    String result = builder.toString();
    // System.out.println("Resu: " + result + "<EOL>");
    // System.out.println("Gold: " + gold + "<EOL>");
    assertTrue(result + " is not equal to " + gold, result.equals(gold) == true);
  }

  public void testReserved() throws Exception {
    String test = "aaa bbb <reserved ccc=\"ddddd\"> eeee </reserved> ffff <reserved ggg=\"hhhh\"/> <other/>";
    Set<String> set = new HashSet<String>();
    set.add("reserved");
    Reader reader = new HTMLStripCharFilter(CharReader.get(new StringReader(test)), set);
    StringBuilder builder = new StringBuilder();
    int ch = 0;
    while ((ch = reader.read()) != -1){
      builder.append((char)ch);
    }
    String result = builder.toString();
    // System.out.println("Result: " + result);
    assertTrue("Escaped tag not preserved: "  + result.indexOf("reserved"), result.indexOf("reserved") == 9);
    assertTrue("Escaped tag not preserved: " + result.indexOf("reserved", 15), result.indexOf("reserved", 15) == 38);
    assertTrue("Escaped tag not preserved: " + result.indexOf("reserved", 41), result.indexOf("reserved", 41) == 54);
    assertTrue("Other tag should be removed", result.indexOf("other") == -1);
  }

  public void testMalformedHTML() throws Exception {
    String test = "a <a hr<ef=aa<a>> </close</a>";
    String gold = "a <a hr<ef=aa > </close ";
    Reader reader = new HTMLStripCharFilter(CharReader.get(new StringReader(test)));
    StringBuilder builder = new StringBuilder();
    int ch = 0;
    while ((ch = reader.read()) != -1){
      builder.append((char)ch);
    }
    String result = builder.toString();
    // System.out.println("Resu: " + result + "<EOL>");
    // System.out.println("Gold: " + gold + "<EOL>");
    assertTrue(result + " is not equal to " + gold + "<EOS>", result.equals(gold) == true);
  }

  public void testBufferOverflow() throws Exception {
    StringBuilder testBuilder = new StringBuilder(HTMLStripCharFilter.DEFAULT_READ_AHEAD + 50);
    testBuilder.append("ah<?> ");
    appendChars(testBuilder, HTMLStripCharFilter.DEFAULT_READ_AHEAD + 500);
    processBuffer(testBuilder.toString(), "Failed on pseudo proc. instr.");//processing instructions

    testBuilder.setLength(0);
    testBuilder.append("<!--");//comments
    appendChars(testBuilder, 3*HTMLStripCharFilter.DEFAULT_READ_AHEAD + 500);//comments have two lookaheads

    testBuilder.append("-->foo");
    processBuffer(testBuilder.toString(), "Failed w/ comment");

    testBuilder.setLength(0);
    testBuilder.append("<?");
    appendChars(testBuilder, HTMLStripCharFilter.DEFAULT_READ_AHEAD + 500);
    testBuilder.append("?>");
    processBuffer(testBuilder.toString(), "Failed with proc. instr.");
    
    testBuilder.setLength(0);
    testBuilder.append("<b ");
    appendChars(testBuilder, HTMLStripCharFilter.DEFAULT_READ_AHEAD + 500);
    testBuilder.append("/>");
    processBuffer(testBuilder.toString(), "Failed on tag");

  }

  private void appendChars(StringBuilder testBuilder, int numChars) {
    int i1 = numChars / 2;
    for (int i = 0; i < i1; i++){
      testBuilder.append('a').append(' ');//tack on enough to go beyond the mark readahead limit, since <?> makes HTMLStripCharFilter think it is a processing instruction
    }
  }  


  private void processBuffer(String test, String assertMsg) throws IOException {
    // System.out.println("-------------------processBuffer----------");
    Reader reader = new HTMLStripCharFilter(CharReader.get(new BufferedReader(new StringReader(test))));//force the use of BufferedReader
    int ch = 0;
    StringBuilder builder = new StringBuilder();
    try {
      while ((ch = reader.read()) != -1){
        builder.append((char)ch);
      }
    } finally {
      // System.out.println("String (trimmed): " + builder.toString().trim() + "<EOS>");
    }
    assertTrue(assertMsg + "::: " + builder.toString() + " is not equal to " + test, builder.toString().equals(test) == true);
  }

  public void testComment() throws Exception {

    String test = "<!--- three dashes, still a valid comment ---> ";
    String gold = "  ";
    Reader reader = new HTMLStripCharFilter(CharReader.get(new BufferedReader(new StringReader(test))));//force the use of BufferedReader
    int ch = 0;
    StringBuilder builder = new StringBuilder();
    try {
      while ((ch = reader.read()) != -1){
        builder.append((char)ch);
      }
    } finally {
      // System.out.println("String: " + builder.toString());
    }
    assertTrue(builder.toString() + " is not equal to " + gold + "<EOS>", builder.toString().equals(gold) == true);
  }


  public void doTestOffsets(String in) throws Exception {
    HTMLStripCharFilter reader = new HTMLStripCharFilter(CharReader.get(new BufferedReader(new StringReader(in))));
    int ch = 0;
    int off = 0;     // offset in the reader
    int strOff = -1; // offset in the original string
    while ((ch = reader.read()) != -1) {
      int correctedOff = reader.correctOffset(off);

      if (ch == 'X') {
        strOff = in.indexOf('X',strOff+1);
        assertEquals(strOff, correctedOff);
      }

      off++;
    }
  }

  public void testOffsets() throws Exception {
    doTestOffsets("hello X how X are you");
    doTestOffsets("hello <p> X<p> how <p>X are you");
    doTestOffsets("X &amp; X &#40; X &lt; &gt; X");

    // test backtracking
    doTestOffsets("X < &zz >X &# < X > < &l > &g < X");
  }

}
