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

import junit.framework.TestCase;

public class HTMLStripCharFilterTest extends TestCase {


  public HTMLStripCharFilterTest(String s) {
    super(s);
  }

  protected void setUp() {
  }

  protected void tearDown() {

  }
  //this is some text  here is a  link  and another  link . This is an entity: & plus a <.  Here is an &
  //
  public void test() throws IOException {
    String html = "<div class=\"foo\">this is some text</div> here is a <a href=\"#bar\">link</a> and " +
            "another <a href=\"http://lucene.apache.org/\">link</a>. " +
            "This is an entity: &amp; plus a &lt;.  Here is an &. <!-- is a comment -->";
    String gold = "                 this is some text       here is a                link     and " +
            "another                                     link    . " +
            "This is an entity: &     plus a <   .  Here is an &.                      ";
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
    assertTrue(gold + " is not equal to " + builder.toString(), gold.equals(builder.toString()) == true);
  }

  //Some sanity checks, but not a full-fledged check
  public void testHTML() throws Exception {

    HTMLStripCharFilter reader = new HTMLStripCharFilter(CharReader.get(new FileReader(new File("htmlStripReaderTest.html"))));
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
    String gold = "\u0393      ";
    Set<String> set = new HashSet<String>();
    set.add("reserved");
    Reader reader = new HTMLStripCharFilter(CharReader.get(new StringReader(test)), set);
    StringBuilder builder = new StringBuilder();
    int ch = 0;
    while ((ch = reader.read()) != -1){
      builder.append((char)ch);
    }
    String result = builder.toString();
    System.out.println("Resu: " + result + "<EOL>");
    System.out.println("Gold: " + gold + "<EOL>");
    assertTrue(result + " is not equal to " + gold + "<EOS>", result.equals(gold) == true);
  }

  public void testEntities() throws Exception {
    String test = "&nbsp; &lt;foo&gt; &#61; &Gamma; bar &#x393;";
    String gold = "       <   foo>    =     \u0393       bar \u0393     ";
    Set<String> set = new HashSet<String>();
    set.add("reserved");
    Reader reader = new HTMLStripCharFilter(CharReader.get(new StringReader(test)), set);
    StringBuilder builder = new StringBuilder();
    int ch = 0;
    while ((ch = reader.read()) != -1){
      builder.append((char)ch);
    }
    String result = builder.toString();
    System.out.println("Resu: " + result + "<EOL>");
    System.out.println("Gold: " + gold + "<EOL>");
    assertTrue(result + " is not equal to " + gold + "<EOS>", result.equals(gold) == true);
  }

  public void testMoreEntities() throws Exception {
    String test = "&nbsp; &lt;junk/&gt; &nbsp; &#33; &#64; and &#8217;";
    String gold = "       <   junk/>           !     @     and ’      ";
    Set<String> set = new HashSet<String>();
    set.add("reserved");
    Reader reader = new HTMLStripCharFilter(CharReader.get(new StringReader(test)), set);
    StringBuilder builder = new StringBuilder();
    int ch = 0;
    while ((ch = reader.read()) != -1){
      builder.append((char)ch);
    }
    String result = builder.toString();
    System.out.println("Resu: " + result + "<EOL>");
    System.out.println("Gold: " + gold + "<EOL>");
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
    System.out.println("Result: " + result);
    assertTrue("Escaped tag not preserved: "  + result.indexOf("reserved"), result.indexOf("reserved") == 9);
    assertTrue("Escaped tag not preserved: " + result.indexOf("reserved", 15), result.indexOf("reserved", 15) == 38);
    assertTrue("Escaped tag not preserved: " + result.indexOf("reserved", 41), result.indexOf("reserved", 41) == 54);
    assertTrue("Other tag should be removed", result.indexOf("other") == -1);
  }

  public void testStrip() throws Exception {
    String test = "{{aaaaaaaa|aaaaaaaaa|aaa [[aaaaaa aaaaaa]] [[aaaaaaaaa]]|aaaaaaaaa (aaaaaa)}}\n" +
            "{{aaaaaaaaa}}\n" +
            "'''aaaaaaaaa''' aa a [[aaaaaaaaa aaaaaaaaaa]] aa aaaaa aa aaaaaaaaa aaa aaaaaaaaa aaaaaaaa aa aaaaaaaaa aa aaa aaaa aa aaaaaaaaaa " +
            "[[aaaaaaaaaa]] ([[aa.]] \"[[aaaaa]]\"<ttt>aaaa aaaaaaaaaaaaa aaaaaaaaaa aaaa aaaaa: \"a aaaaaaaaaaa aaaa aa aaaaaaaa aa aaa aaaaaaaaa " +
            "aaaaa aa aaa aaaaaaaaaa aaaaaaa ''aaa aaaaaaaaaa'', aaaaaaaaa aa aaa aaaaa, aaa ''aaaaaaaaaa'', aaaaaaaaa aa aaa aaaaaaaaaaaaaa aa a aaaaaaaaa aaaaaa. aaaaaaaaaa, aaaa aaaaaaaa, " +
            "aaaa aa aaa aaa aaaa aaaaaaaaaa aa a aaaaaaa aaa aaaaa, aaa aaaa aa aaaaaaaa aa aaaaaaaaa'a ''a aaaaaa'' aaaaaaaaaa aa aaa aaaaa aa aaa aaa aaaaaaa aa aaaaaaaaaa aa aaaa aaa aaa " +
            "aaaa aa a aaaaaaaaa aaaaa aaaa aaaaaa aaa aaaaaaa aaaaaaaaa, aaa aa aaaaaaaaa, aaa aaaaa aa aaa aaaaaaaa. aaaaaaaaa aaaaaaa aaa aaaa aa aaaaaaa, aaaaaaaaaaa aaaaaaaaa aaaaaaaaa " +
            "aaa aaaa aaaaaaaa aa aaa aaaaa.\" -aaaaaaa, aaaa. aaaaaaaaa," +
            " aaaaaaaa aaaaa aaaa, a. aa-aa</ttt>) aaa aaaaaaaaaa aaa aaaaaaaaaaa.<ttt bbbb=bbbbbbbbbbb>''aaaaaaaaa''. aaaaaaaa¾aaa aaaaaaaaaa. aaaa. aaaaaaaa¾aaa aaaaaaaaaa aaaaaaa aaaaaaa. " +
            "[[aa aaaaaa]] [[aaaa]] <tttb://ccc.cccccccccc.ccc/cc/ccccccc-ccccccc>. aaaaaaaaa aa \"a aaaaaaa aa aaaaaaaaa aaa aaaaaaaaa aaaaaaa aa aaa aaaaaa aaaa aaaaaaaaaa aa aaaa aaaaaaa aaa " +
            "aaaaaaaaaaa.\"</ttt><ttt dddd=dddddddddddd>''aaaaaaaaa''. aaa aaaaaaa aaaaaaaaa aaaaaaaaaaaa aa aaaaaaaaaa. aaaa. a. aa" +
            " \"aaaaaaaaa aa aaa aaaa aaaa a aaaaaaa aaaaaaa aaa aaaaa, aa aaaaaaaaaa, aa aaaa aaaaaaaa aaa aaaaaaaaa.\"</ttt> aaa aaaa \"aaaaaaaaa\" " +
            "aa [[aaaaaaaaa|aaaaaaa aaaa]] aaa [[aaaaa aaaaaaaa|aaaaa]] ''[[aaaaaaaaaa:???????|???????]]'' (\"aaaaaaa [[aaaaaa]]a\" aa \"aaaaaaa aaaaaa\")." +
            " aaaa \"aaaaaaaaa\", aa aaa aaaa aaaaaaa aaaaaaa, aa aaa aaaaaa aaaa aaa aaaaa aa [[aaaaaaaaa]] (aaa aaaa aaaa [[aaaaaaaaaaa aaaaaaaaa]]) aaa " +
            "aaaaaaaaaaa aaa aaaaaa aa aaaaaaaaa. \n" +
            "\n" +
            "aaaaa aaa a aaaaaaa aa aaaaa aaa aaaaaaaaaa aa aaaaaaaaa aaaa aaaaaaa aaaaaa aa aaaaaaaaaa.<ttt>aaaaaaaaaa, aaaa aaaaaaaaaaa. ''aaaaaaaaa: a " +
            "aaaaaaaaaa aa aaaaaaaaaaaaa aaaaaaaa'', aaaaaaa aaaaa aaaaaaaaaaaa, aaaa, a.a</ttt><tttt>{{aaaa aaaaaaa|aaaaaa=a.a. aaaaaa|aaaaa=aaa aaaaaaaaa " +
            "aaaaaaaaa aa aaaaaaaaa aaaaaaa|aaaa=aaaa|aaaaaaa=aaaaaaa aaaaaaaaa aaaaaaaaa|aaaaaa=aa|aaaaa=a|aaaaa=aaa-aaa|aaa=aa.aaaa/aaaaaa}}</ttt> aaaaaaa," +
            " aaa aaaaaaaaa aaa aaa aaaaaaaaaaaa aaaa aaaaaaaaaaaaa aaa aaa aaa aa aaaa aaa aaaaaaaa aaaaaaaaa.<ttt>aaaaaa, aaaaaaa. aaaaaaaaa. a aaaaaaaaa aa " +
            "aaaaaaaaaaaa aaaaaaaaa aaaaaaaaaa, aaaaaaa aaaaaaa, aaaaaa a. aaa aaaaaa, aaaaaa. aaaaaaaaa aaaaaaaaaa, aaaa, a.aaa</ttt> aaaaa aaaa aaa aaaaaaaaaaa" +
            " aaaaa, \"aaaaa aa aa aaaaaa aaaaaaaa aaaaaaaa aaaa aaa aaaaaaaaaa aaaa, aaa aaaaa aaaaaaaaaa aaaaaaaaaa aa aaaa aaaaa a aaaaaaa [[aaaaaa aaaaaaaaaaa]].\"<ttt>aaaaaaaaa. " +
            "aaa aaaaaa aaaaaaaaa aa aaaaaaaaaa, aaaaaa aaaaaaaaaa aaaaa, aaaa, a. aa</ttt> aaaaaaaaa aaaaaaaa aaaaaaa aaa aaa aa aaa aaaa aaaaa aa aaaaaaaaaaaa aaa aaaaaaaaaa.<ttt>aaaaaaaa, " +
            "aaaaaa aaaaaaa \"aaaaaaaaa aaaaaaaa aaa aaa aaaaaaa aaaaa aaaaaaaa aa aaaaa, aaaa-aaaa\" [a. aaa]</ttt>\n" +
            "==aaaaaaa==\n" +
            "===aaa-aaaaaaaaaa aaaaaaa===\n" +
            "{{aaaa|aaaaaaa aa aaaaaaaaa}}\n" +
            "{{aaaaa aa aaaaaaaaaa}}\n" +
            "aaaaaaaaaa aa aaa aaaaa aaa aaaaaaaaaaaa aaaaaaaaa aaa a aaaa aaaaaaa aaaaa aa aaa aaaaaaaaa aa aaa aaaaaaaaa aaaaaaaa aa aaaaaaaaaa aaaaaaa aaaaaa. aaaa aaaaa aaaa aaaaaaaaa aaaaaa " +
            "aaa aa aaaaaaaa aa aaaaa aa aaa aa aaaaa aa aaa [[aaaaaa]] aaaa [[aaaaa|aaa aaa]],<ttt eeee=\"eeeeee\">aaaaa aaaaaaaaa, [aaaa://aaaaaaaa.aaaaaa.aaa/aaaaaaaaaaaaaaaaaa/aaaaaaaaa/aaaaaaaaaaaaaaaa.aaaa " +
            "\"aaaaaaaaa\", aaaa aaa aaaaaaaaaaaaa aaaaaaaaaa, aaaa]</ttt> aaaaaa aaaa aa a aaaaaaaaaaaaa" +
            " aaaaa.<ttt>{{aaaaaaa|[aaaa://aaa.aaaaaaaaaaaaaaaaa.aaa/aaa/aaaaaa/aaaaa--aaaaaaaaa.aaa]|aa.a&aaaa;[[aaaaaaaa|aaa]]" +
            "<!-- ggggggggggg/ggg, gggggg ggggg -->}}</aaa> [[aaaa aa aaaaaa]], aaa aaaaaaa aa [[aaaaaaaa]] aaaa aaaaaaaaaa aaaaaa aaaaa aaaaaaa aaaaaaaaa aaaaaa.<ttt ffff=\"ffffff\"/>\n" +
            "\n" +
            "aaaaaaaaa aa aaa aaaaaa aaaaa, aaaaaaa, aaa aaa aaaaa aa aaa aaaaaaa aaaaaaaaa aaaaaaa aa aaa [[aaa aa aaaaaaaaaaaaa|aaaaaaaaaaaaa]], aaaaaaaaaaaa [[aaaaaaaa]]'a aaaaaaaaa aaa aaa aaaaa " +
            "aaaaaaaaaa aa aaaaaaa.<ttt hhhh=hhhhhhh>''aaaaaaaaa'', aaaaaaaaa¨ aaaaaaa¨ aaaaaa aaaaaaaaaaaa aaaa (aa aaaaaaa) aaaa://aa.aaaaaaa.aaa.aaa © aaaa-aaaa aaaaaaaaa aaaaaaaaaaa. aaa aaaaaa aaaaaaaa\n" +
            "</ttt> aaa aaaa \"aaaaaaaaa\" aaa aaaaaaaaaa aaaa aa a aaaa aa [[aaaaa]], aaa aa aaa [[aaaaaa aaaaaaaaaa]] aaaa aaaaaa aaaa aa aaa ''aaaaaŽa'' aaa aaaaaaa aa aaa aaa aaaa aa a aaaaaaaa " +
            "aaaaa,<ttt>aaaaaaa, aaaa. ''aaaaaaaaa'', aaaaaa: aaaaaaaa aaaaa aaa., aaaa. aa. aa</ttt> aaaaaa aaa [[aaaaaaa (aaaaaaaa)|aaaaaaa]] aaaaaaa aa a \"aaaaaaaaaaaaa aaaaaaaaaa\" aa aa [[aaaaaaaa]]. " +
            "aa aaa aa aaaa aaaaaaaaa aaaaaaa aaaa [[aaaaaaa aaaaaa]] aaaaa aaaaaaa aaa aaaaaaaaaa, aaaaa aa aaaaaaaaaa aa aaaa aa aa aaa aaaaa aaaaaaaaaa aa aaaaaa aaaaaaaaa aaaaaaa." +
            "<ttt>[aaaa://aaaaa.aaaaaaaa.aaa/aaaaaaa/aaaaaa/ aaaaaaa aaaaaa] aaaaaaaa aaaaaaaaaaaa aa aaaaaaaaaa, aaaaa aaaaaaaaa aaa [[aa aaaaaaa]] [[aaaa]]; aaaaaaaaaaa aaaaaaaa aaa [[aaa aa]] [[aaaa]]</ttt>\n" +
            "\n" +
            "[[aaaaa aaaaaaaaaa]] aa ''[[aaa aaaaaaaaaaaa aa aaaaaa]]'' (aaaa) aaaa aaa aaaa [[zzzzzzz]] aa aaaaaaaa";
    Reader reader = new HTMLStripCharFilter(CharReader.get(new StringReader(test)));
    Reader noStrip = new StringReader(test);
    int ch = 0;
    int ch2 = 0;
    int i = 0;
    StringBuilder builder = new StringBuilder();
    while ((ch = reader.read()) != -1 && (ch2 = noStrip.read()) != -1){
      //System.out.println("char[" + i + "] = '" + (char)ch + "' NS: '" + (char)ch2 + "'" + ((ch != ch2 && (ch2 != 't' || ch2 != '<' || ch2 != '>')) ? "<<<<<<<<<<<<<<<<<<<<<<<<" : ""));
      assertTrue(ch + " does not equal: " + "t or < or > ::: String: " + builder.toString(), ch == ch2 || ch == ' '/*&& ch != '<' && ch != '>'*/);
      builder.append((char)ch);
      i++;
    }
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
    System.out.println("-------------------processBuffer----------");
    Reader reader = new HTMLStripCharFilter(CharReader.get(new BufferedReader(new StringReader(test))));//force the use of BufferedReader
    int ch = 0;
    StringBuilder builder = new StringBuilder();
    try {
      while ((ch = reader.read()) != -1){
        builder.append((char)ch);
      }
    } finally {
      System.out.println("String (trimmed): " + builder.toString().trim() + "<EOS>");
    }
    assertTrue(assertMsg + "::: " + builder.toString() + " is not equal to " + test, builder.toString().equals(test) == true);
  }

  public void testComment() throws Exception {

    String test = "<!--- three dashes, still a valid comment ---> ";
    String gold = "                                               ";
    Reader reader = new HTMLStripCharFilter(CharReader.get(new BufferedReader(new StringReader(test))));//force the use of BufferedReader
    int ch = 0;
    StringBuilder builder = new StringBuilder();
    try {
      while ((ch = reader.read()) != -1){
        builder.append((char)ch);
      }
    } finally {
      System.out.println("String: " + builder.toString());
    }
    assertTrue(builder.toString() + " is not equal to " + gold + "<EOS>", builder.toString().equals(gold) == true);
  }

}
