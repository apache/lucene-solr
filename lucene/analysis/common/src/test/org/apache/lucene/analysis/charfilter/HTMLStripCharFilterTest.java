package org.apache.lucene.analysis.charfilter;

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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.util._TestUtil;

public class HTMLStripCharFilterTest extends BaseTokenStreamTestCase {

  static private Analyzer newTestAnalyzer() {
    return new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
        return new TokenStreamComponents(tokenizer, tokenizer);
      }

      @Override
      protected Reader initReader(String fieldName, Reader reader) {
        return new HTMLStripCharFilter(reader);
      }
    };
  }

  //this is some text  here is a  link  and another  link . This is an entity: & plus a <.  Here is an &
  //
  public void test() throws IOException {
    String html = "<div class=\"foo\">this is some text</div> here is a <a href=\"#bar\">link</a> and " +
            "another <a href=\"http://lucene.apache.org/\">link</a>. " +
            "This is an entity: &amp; plus a &lt;.  Here is an &. <!-- is a comment -->";
    String gold = "\nthis is some text\n here is a link and " +
            "another link. " +
            "This is an entity: & plus a <.  Here is an &. ";
    HTMLStripCharFilter reader = new HTMLStripCharFilter(new StringReader(html));
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
    assertEquals("'" + builder.toString() + "' is not equal to '" + gold + "'",
                 gold, builder.toString());
  }

  //Some sanity checks, but not a full-fledged check
  public void testHTML() throws Exception {
    InputStream stream = getClass().getResourceAsStream("htmlStripReaderTest.html");
    HTMLStripCharFilter reader = new HTMLStripCharFilter(new InputStreamReader(stream, "UTF-8"));
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

  public void testMSWord14GeneratedHTML() throws Exception {
    InputStream stream = getClass().getResourceAsStream("MS-Word 14 generated.htm");
    HTMLStripCharFilter reader = new HTMLStripCharFilter(new InputStreamReader(stream, "UTF-8"));
    String gold = "This is a test";
    StringBuilder builder = new StringBuilder();
    int ch = 0;
    try {
      while ((ch = reader.read()) != -1){
        builder.append((char)ch);
      }
    } finally {
      // System.out.println("String: " + builder.toString());
    }
    assertEquals("'" + builder.toString().trim() + "' is not equal to '" + gold + "'",
                 gold, builder.toString().trim());
  }
  
  
  public void testGamma() throws Exception {
    String test = "&Gamma;";
    String gold = "\u0393";
    Set<String> set = new HashSet<String>();
    set.add("reserved");
    Reader reader = new HTMLStripCharFilter(new StringReader(test), set);
    StringBuilder builder = new StringBuilder();
    int ch = 0;
    while ((ch = reader.read()) != -1){
      builder.append((char)ch);
    }
    String result = builder.toString();
    assertEquals("'" + result + "' is not equal to '" + gold + "<EOS>'", gold, result);
  }

  public void testEntities() throws Exception {
    String test = "&nbsp; &lt;foo&gt; &Uuml;bermensch &#61; &Gamma; bar &#x393;";
    String gold = "  <foo> \u00DCbermensch = \u0393 bar \u0393";
    Set<String> set = new HashSet<String>();
    set.add("reserved");
    Reader reader = new HTMLStripCharFilter(new StringReader(test), set);
    StringBuilder builder = new StringBuilder();
    int ch = 0;
    while ((ch = reader.read()) != -1){
      builder.append((char)ch);
    }
    String result = builder.toString();
    assertEquals("'" + result + "' is not equal to '" + gold + "<EOS>'", gold, result);
  }

  public void testMoreEntities() throws Exception {
    String test = "&nbsp; &lt;junk/&gt; &nbsp; &#33; &#64; and &#8217;";
    String gold = "  <junk/>   ! @ and ’";
    Set<String> set = new HashSet<String>();
    set.add("reserved");
    Reader reader = new HTMLStripCharFilter(new StringReader(test), set);
    StringBuilder builder = new StringBuilder();
    int ch = 0;
    while ((ch = reader.read()) != -1){
      builder.append((char)ch);
    }
    String result = builder.toString();
    assertEquals("'" + result + "' is not equal to '" + gold + "<EOS>'", gold, result);
  }

  public void testReserved() throws Exception {
    String test = "aaa bbb <reserved ccc=\"ddddd\"> eeee </reserved> ffff <reserved ggg=\"hhhh\"/> <other/>";
    Set<String> set = new HashSet<String>();
    set.add("reserved");
    Reader reader = new HTMLStripCharFilter(new StringReader(test), set);
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
    String[] testGold = {
        "a <a hr<ef=aa<a>> </close</a>",
        "a <a hr<ef=aa> </close",

        "<a href=http://dmoz.org/cgi-bin/add.cgi?where=/arts/\" class=lu style=\"font-size: 9px\" target=dmoz>Submit a Site</a>",
        "Submit a Site",

        "<a href=javascript:ioSwitch('p8','http://www.csmonitor.com/') title=expand id=e8 class=expanded rel=http://www.csmonitor.com/>Christian Science",
        "Christian Science",

        "<link rel=\"alternate\" type=\"application/rss+xml\" title=\"San Francisco \" 2008 RSS Feed\" href=\"http://2008.sf.wordcamp.org/feed/\" />",
        "\n",

        // "<" before ">" inhibits tag recognition
        "<a href=\" http://www.surgery4was.happyhost.org/video-of-arthroscopic-knee-surgery symptoms.html, heat congestive heart failure <a href=\" http://www.symptoms1bad.happyhost.org/canine",
        "<a href=\" http://www.surgery4was.happyhost.org/video-of-arthroscopic-knee-surgery symptoms.html, heat congestive heart failure <a href=\" http://www.symptoms1bad.happyhost.org/canine",

        "<a href=\"http://ucblibraries.colorado.edu/how/index.htm\"class=\"pageNavAreaText\">",
        "",

        "<link title=\"^\\\" 21Sta's Blog\" rel=\"search\"  type=\"application/opensearchdescription+xml\"  href=\"http://21sta.com/blog/inc/opensearch.php\" />",
        "\n",

        "<a href=\"#postcomment\" title=\"\"Leave a comment\";\">?",
        "?",

        "<a href='/modern-furniture'   ' id='21txt' class='offtab'   onMouseout=\"this.className='offtab';  return true;\" onMouseover=\"this.className='ontab';  return true;\">",
        "",

        "<a href='http://alievi.wordpress.com/category/01-todos-posts/' style='font-size: 275%; padding: 1px; margin: 1px;' title='01 - Todos Post's (83)'>",
        "",

        "The <a href=<a href=\"http://www.advancedmd.com>medical\">http://www.advancedmd.com>medical</a> practice software</a>",
        "The <a href=medical\">http://www.advancedmd.com>medical practice software",

        "<a href=\"node/21426\" class=\"clipTitle2\" title=\"Levi.com/BMX 2008 Clip of the Week 29 \"Morgan Wade Leftover Clips\"\">Levi.com/BMX 2008 Clip of the Week 29...",
        "Levi.com/BMX 2008 Clip of the Week 29...",

        "<a href=\"printer_friendly.php?branch=&year=&submit=go&screen=\";\">Printer Friendly",
        "Printer Friendly",

        "<a href=#\" ondragstart=\"return false\" onclick=\"window.external.AddFavorite('http://www.amazingtextures.com', 'Amazing Textures');return false\" onmouseover=\"window.status='Add to Favorites';return true\">Add to Favorites",
        "Add to Favorites",

        "<a href=\"../at_home/at_home_search.html\"../_home/at_home_search.html\">At",
        "At",

        "E-mail: <a href=\"\"mailto:XXXXXX@example.com\" \">XXXXXX@example.com </a>",
        "E-mail: XXXXXX@example.com ",

        "<li class=\"farsi\"><a title=\"A'13?\" alt=\"A'13?\" href=\"http://www.america.gov/persian\" alt=\"\" name=\"A'13?\"A'13? title=\"A'13?\">A'13?</a></li>",
        "\nA'13?\n",

        "<li><a href=\"#28\" title=\"Hubert \"Geese\" Ausby\">Hubert \"Geese\" Ausby</a></li>",
        "\nHubert \"Geese\" Ausby\n",

        "<href=\"http://anbportal.com/mms/login.asp\">",
        "\n",

        "<a href=\"",
        "<a href=\"",

        "<a href=\">",
        "",

        "<a rel=\"nofollow\" href=\"http://anissanina31.skyrock.com/1895039493-Hi-tout-le-monde.html\" title=\" Hi, tout le monde !>#</a>",
        "#",

        "<a href=\"http://annunciharleydavidsonusate.myblog.it/\" title=\"Annunci Moto e Accessori Harley Davidson\" target=\"_blank\"><img src=\"http://annunciharleydavidsonusate.myblog.it/images/Antipixel.gif\" /></a>",
        "",

        "<a href=\"video/addvideo&v=120838887181\" onClick=\"return confirm('Are you sure you want  add this video to your profile? If it exists some video in your profile will be overlapped by this video!!')\" \" onmouseover=\"this.className='border2'\" onmouseout=\"this.className=''\">",
        "",

        "<a href=#Services & Support>",
        "",

        // "<" and ">" chars are accepted in on[Event] attribute values
        "<input type=\"image\" src=\"http://apologyindex.com/ThemeFiles/83401-72905/images/btn_search.gif\"value=\"Search\" name=\"Search\" alt=\"Search\" class=\"searchimage\" onclick=\"incom ='&sc=' + document.getElementById('sel').value ; var dt ='&dt=' + document.getElementById('dt').value; var searchKeyword = document.getElementById('q').value ; searchKeyword = searchKeyword.replace(/\\s/g,''); if (searchKeyword.length < 3){alert('Nothing to search. Search keyword should contain atleast 3 chars.'); return false; } var al='&al=' +  document.getElementById('advancedlink').style.display ;  document.location.href='http://apologyindex.com/search.aspx?q=' + document.getElementById('q').value + incom + dt + al;\" />",
        "",

        "<input type=\"image\" src=\"images/afbe.gif\" width=\"22\" height=\"22\"  hspace=\"4\" title=\"Add to Favorite\" alt=\"Add to Favorite\"onClick=\" if(window.sidebar){ window.sidebar.addPanel(document.title,location.href,''); }else if(window.external){ window.external.AddFavorite(location.href,document.title); }else if(window.opera&&window.print) { return true; }\">",
        "",

        "<area shape=\"rect\" coords=\"12,153,115,305\" href=\"http://statenislandtalk.com/v-web/gallery/Osmundsen-family\"Art's Norwegian Roots in Rogaland\">",
        "\n",

        "<a rel=\"nofollow\" href=\"http://arth26.skyrock.com/660188240-bonzai.html\" title=\"bonza>#",
        "#",

        "<a href=  >",
        "",

        "<ahref=http:..",
        "<ahref=http:..",

        "<ahref=http:..>",
        "\n",

        "<ahref=\"http://aseigo.bddf.ca/cms/1025\">A",
        "\nA",

        "<a href=\"javascript:calendar_window=window.open('/calendar.aspx?formname=frmCalendar.txtDate','calendar_window','width=154,height=188');calendar_window.focus()\">",
        "",

        "<a href=\"/applications/defenseaerospace/19+rackmounts\" title=\"19\" Rackmounts\">",
        "",

        "<a href=http://www.azimprimerie.fr/flash/backup/lewes-zip-code/savage-model-110-manual.html title=savage model 110 manual rel=dofollow>",
        "",

        "<a class=\"at\" name=\"Lamborghini  href=\"http://lamborghini.coolbegin.com\">Lamborghini /a>",
        "Lamborghini /a>",

        "<A href='newslink.php?news_link=http%3A%2F%2Fwww.worldnetdaily.com%2Findex.php%3Ffa%3DPAGE.view%26pageId%3D85729&news_title=Florida QB makes 'John 3:16' hottest Google search Tebow inscribed Bible reference on eye black for championship game' TARGET=_blank>",
        "",

        "<a href=/myspace !style='color:#993333'>",
        "",

        "<meta name=3DProgId content=3DExcel.Sheet>",
        "\n",

        "<link id=3D\"shLink\" href=3D\"PSABrKelly-BADMINTONCupResults08FINAL2008_09_19=_files/sheet004.htm\">",
        "\n",

        "<td bgcolor=3D\"#FFFFFF\" nowrap>",
        "\n",

        "<a href=\"http://basnect.info/usersearch/\"predicciones-mundiales-2009\".html\">\"predicciones mundiales 2009\"</a>",
        "\"predicciones mundiales 2009\"",

        "<a class=\"comment-link\" href=\"https://www.blogger.com/comment.g?blogID=19402125&postID=114070605958684588\"location.href=https://www.blogger.com/comment.g?blogID=19402125&postID=114070605958684588;>",
        "",

        "<a href = \"/videos/Bishop\"/\" title = \"click to see more Bishop\" videos\">Bishop\"</a>",
        "Bishop\"",

        "<a href=\"http://bhaa.ie/calendar/event.php?eid=20081203150127531\"\">BHAA Eircom 2 &amp; 5 miles CC combined start</a>",
        "BHAA Eircom 2 & 5 miles CC combined start",

        "<a href=\"http://people.tribe.net/wolfmana\" onClick='setClick(\"Application[tribe].Person[bb7df210-9dc0-478c-917f-436b896bcb79]\")'\" title=\"Mana\">",
        "",

        "<a  href=\"http://blog.edu-cyberpg.com/ct.ashx?id=6143c528-080c-4bb2-b765-5ec56c8256d3&url=http%3a%2f%2fwww.gsa.ac.uk%2fmackintoshsketchbook%2f\"\" eudora=\"autourl\">",
        "",

        // "<" before ">" inhibits tag recognition
        "<input type=\"text\" value=\"<search here>\">",
        "<input type=\"text\" value=\"\n\">",

        "<input type=\"text\" value=\"<search here\">",
        "<input type=\"text\" value=\"\n",

        "<input type=\"text\" value=\"search here>\">",
        "\">",

        // "<" and ">" chars are accepted in on[Event] attribute values
        "<input type=\"text\" value=\"&lt;search here&gt;\" onFocus=\"this.value='<search here>'\">",
        "",

        "<![if ! IE]>\n<link href=\"http://i.deviantart.com/icons/favicon.png\" rel=\"shortcut icon\"/>\n<![endif]>",
        "\n\n\n",

        "<![if supportMisalignedColumns]>\n<tr height=0 style='display:none'>\n<td width=64 style='width:48pt'></td>\n</tr>\n<![endif]>",
        "\n\n\n\n\n\n\n\n",
    };
    for (int i = 0 ; i < testGold.length ; i += 2) {
      String test = testGold[i];
      String gold = testGold[i + 1];
      Reader reader = new HTMLStripCharFilter(new StringReader(test));
      StringBuilder builder = new StringBuilder();
      int ch = 0;
      while ((ch = reader.read()) != -1){
        builder.append((char)ch);
      }
      String result = builder.toString();
      assertEquals("Test: '" + test + "'", gold, result);
    }
  }


  public void testBufferOverflow() throws Exception {
    StringBuilder testBuilder = new StringBuilder(HTMLStripCharFilter.getInitialBufferSize() + 50);
    testBuilder.append("ah<?> ??????");
    appendChars(testBuilder, HTMLStripCharFilter.getInitialBufferSize() + 500);
    processBuffer(testBuilder.toString(), "Failed on pseudo proc. instr.");//processing instructions

    testBuilder.setLength(0);
    testBuilder.append("<!--");//comments
    appendChars(testBuilder, 3 * HTMLStripCharFilter.getInitialBufferSize() + 500);//comments have two lookaheads

    testBuilder.append("-->foo");
    String gold = "foo";
    Reader reader = new HTMLStripCharFilter(new StringReader(testBuilder.toString()));
    int ch = 0;
    StringBuilder builder = new StringBuilder();
    try {
      while ((ch = reader.read()) != -1){
        builder.append((char)ch);
      }
    } finally {
      // System.out.println("String: " + builder.toString());
    }
    assertEquals("'" + builder.toString() + "' is not equal to '" + gold + "'",
                 gold, builder.toString());

    testBuilder.setLength(0);
    testBuilder.append("<?");
    appendChars(testBuilder, HTMLStripCharFilter.getInitialBufferSize() + 500);
    testBuilder.append("?>");
    gold = "";
    reader = new HTMLStripCharFilter(new StringReader(testBuilder.toString()));
    ch = 0;
    builder = new StringBuilder();
    try {
      while ((ch = reader.read()) != -1){
        builder.append((char)ch);
      }
    } finally {
      // System.out.println("String: " + builder.toString());
    }
    assertEquals("'" + builder.toString() + "' is not equal to '" + gold + "'",
                 gold, builder.toString());
    
    testBuilder.setLength(0);
    testBuilder.append("<b ");
    appendChars(testBuilder, HTMLStripCharFilter.getInitialBufferSize() + 500);
    testBuilder.append("/>");
    gold = "";
    reader = new HTMLStripCharFilter(new StringReader(testBuilder.toString()));
    ch = 0;
    builder = new StringBuilder();
    try {
      while ((ch = reader.read()) != -1){
        builder.append((char)ch);
      }
    } finally {
      // System.out.println("String: " + builder.toString());
    }
    assertEquals("'" + builder.toString() + "' is not equal to '" + gold + "'",
                 gold, builder.toString());
  }

  private void appendChars(StringBuilder testBuilder, int numChars) {
    int i1 = numChars / 2;
    for (int i = 0; i < i1; i++){
      testBuilder.append('a').append(' ');//tack on enough to go beyond the mark readahead limit, since <?> makes HTMLStripCharFilter think it is a processing instruction
    }
  }  


  private void processBuffer(String test, String assertMsg) throws IOException {
    // System.out.println("-------------------processBuffer----------");
    Reader reader = new HTMLStripCharFilter(new BufferedReader(new StringReader(test)));//force the use of BufferedReader
    int ch = 0;
    StringBuilder builder = new StringBuilder();
    try {
      while ((ch = reader.read()) != -1){
        builder.append((char)ch);
      }
    } finally {
      // System.out.println("String (trimmed): " + builder.toString().trim() + "<EOS>");
    }
    assertEquals(assertMsg + "::: " + builder.toString() + " is not equal to " + test,
        test, builder.toString());
  }

  public void testComment() throws Exception {

    String test = "<!--- three dashes, still a valid comment ---> ";
    String gold = " ";
    Reader reader = new HTMLStripCharFilter(new BufferedReader(new StringReader(test)));//force the use of BufferedReader
    int ch = 0;
    StringBuilder builder = new StringBuilder();
    try {
      while ((ch = reader.read()) != -1){
        builder.append((char)ch);
      }
    } finally {
      // System.out.println("String: " + builder.toString());
    }
    assertEquals("'" + builder.toString() + "' is not equal to '" + gold + "'",
                 gold, builder.toString());
  }


  public void doTestOffsets(String in) throws Exception {
    HTMLStripCharFilter reader = new HTMLStripCharFilter(new BufferedReader(new StringReader(in)));
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
//    doTestOffsets("hello X how X are you");
    doTestOffsets("hello <p> X<p> how <p>X are you");
    doTestOffsets("X &amp; X &#40; X &lt; &gt; X");

    // test backtracking
    doTestOffsets("X < &zz >X &# < X > < &l > &g < X");
  }

  static void assertLegalOffsets(String in) throws Exception {
    int length = in.length();
    HTMLStripCharFilter reader = new HTMLStripCharFilter(new BufferedReader(new StringReader(in)));
    int ch = 0;
    int off = 0;
    while ((ch = reader.read()) != -1) {
      int correction = reader.correctOffset(off);
      assertTrue("invalid offset correction: " + off + "->" + correction + " for doc of length: " + length,
          correction <= length);
      off++;
    }
  }

  public void testLegalOffsets() throws Exception {
    assertLegalOffsets("hello world");
    assertLegalOffsets("hello &#x world");
  }

  public void testRandom() throws Exception {
    int numRounds = RANDOM_MULTIPLIER * 1000;
    checkRandomData(random(), newTestAnalyzer(), numRounds);
  }
  
  public void testRandomHugeStrings() throws Exception {
    int numRounds = RANDOM_MULTIPLIER * 100;
    checkRandomData(random(), newTestAnalyzer(), numRounds, 8192);
  }

  public void testCloseBR() throws Exception {
    checkAnalysisConsistency(random(), newTestAnalyzer(), random().nextBoolean(), " Secretary)</br> [[M");
  }
  
  public void testServerSideIncludes() throws Exception {
    String test = "one<img src=\"image.png\"\n"
        + " alt =  \"Alt: <!--#echo var='${IMAGE_CAPTION:<!--comment-->\\'Comment\\'}'  -->\"\n\n"
        + " title=\"Title: <!--#echo var=\"IMAGE_CAPTION\"-->\">two";
    String gold = "onetwo";
    Reader reader = new HTMLStripCharFilter(new StringReader(test));
    int ch = 0;
    StringBuilder builder = new StringBuilder();
    try {
      while ((ch = reader.read()) != -1){
        builder.append((char)ch);
      }
    } finally {
      // System.out.println("String: " + builder.toString());
    }
    assertTrue(builder.toString() + " is not equal to " + gold, builder.toString().equals(gold));

    test = "one<script><!-- <!--#config comment=\"<!-- \\\"comment\\\"-->\"--> --></script>two";
    gold = "one\ntwo";
    reader = new HTMLStripCharFilter(new StringReader(test));
    ch = 0;
    builder = new StringBuilder();
    try {
      while ((ch = reader.read()) != -1){
        builder.append((char)ch);
      }
    } finally {
      // System.out.println("String: " + builder.toString());
    }
    assertEquals("'" + builder.toString() + "' is not equal to '" + gold + "'",
                 gold, builder.toString());
  }

  public void testScriptQuotes() throws Exception {
    String test = "one<script attr= bare><!-- action('<!-- comment -->', \"\\\"-->\\\"\"); --></script>two";
    String gold = "one\ntwo";
    Reader reader = new HTMLStripCharFilter(new StringReader(test));
    int ch = 0;
    StringBuilder builder = new StringBuilder();
    try {
      while ((ch = reader.read()) != -1){
        builder.append((char)ch);
      }
    } finally {
      // System.out.println("String: " + builder.toString());
    }
    assertEquals("'" + builder.toString() + "' is not equal to '" + gold + "'",
        gold, builder.toString());

    test = "hello<script><!-- f('<!--internal--></script>'); --></script>";
    gold = "hello\n";
    reader = new HTMLStripCharFilter(new StringReader(test));
    ch = 0;
    builder = new StringBuilder();
    try {
      while ((ch = reader.read()) != -1){
        builder.append((char)ch);
      }
    } finally {
      // System.out.println("String: " + builder.toString());
    }
    assertEquals("'" + builder.toString() + "' is not equal to '" + gold + "'",
                 gold, builder.toString());
  }

  public void testEscapeScript() throws Exception {
    String test = "one<script no-value-attr>callSomeMethod();</script>two";
    String gold = "one<script no-value-attr></script>two";
    Set<String> escapedTags = new HashSet<String>(Arrays.asList("SCRIPT"));
    Reader reader = new HTMLStripCharFilter
        (new StringReader(test), escapedTags);
    int ch = 0;
    StringBuilder builder = new StringBuilder();
    try {
      while ((ch = reader.read()) != -1){
        builder.append((char)ch);
      }
    } finally {
      // System.out.println("String: " + builder.toString());
    }
    assertEquals("'" + builder.toString() + "' is not equal to '" + gold + "'",
                 gold, builder.toString());
  }

  public void testStyle() throws Exception {
    String test = "one<style type=\"text/css\">\n"
                + "<!--\n"
                + "@import url('http://www.lasletrasdecanciones.com/css.css');\n"
                + "-->\n"
                + "</style>two";
    String gold = "one\ntwo";
    Reader reader = new HTMLStripCharFilter(new StringReader(test));
    int ch = 0;
    StringBuilder builder = new StringBuilder();
    try {
      while ((ch = reader.read()) != -1){
        builder.append((char)ch);
      }
    } finally {
      // System.out.println("String: " + builder.toString());
    }
    assertEquals("'" + builder.toString() + "' is not equal to '" + gold + "'",
        gold, builder.toString());
  }

  public void testEscapeStyle() throws Exception {
    String test = "one<style type=\"text/css\"> body,font,a { font-family:arial; } </style>two";
    String gold = "one<style type=\"text/css\"></style>two";
    Set<String> escapedTags = new HashSet<String>(Arrays.asList("STYLE"));
    Reader reader = new HTMLStripCharFilter
        (new StringReader(test), escapedTags);
    int ch = 0;
    StringBuilder builder = new StringBuilder();
    try {
      while ((ch = reader.read()) != -1){
        builder.append((char)ch);
      }
    } finally {
      // System.out.println("String: " + builder.toString());
    }
    assertEquals("'" + builder.toString() + "' is not equal to '" + gold + "'",
        gold, builder.toString());
  }

  public void testBR() throws Exception {
    String[] testGold = {
        "one<BR />two<br>three",
        "one\ntwo\nthree",

        "one<BR some stuff here too>two</BR>",
        "one\ntwo\n",
    };
    for (int i = 0 ; i < testGold.length ; i += 2) {
      String test = testGold[i];
      String gold = testGold[i + 1];
      Reader reader = new HTMLStripCharFilter(new StringReader(test));
      StringBuilder builder = new StringBuilder();
      int ch = 0;
      while ((ch = reader.read()) != -1){
        builder.append((char)ch);
      }
      String result = builder.toString();
      assertEquals("Test: '" + test + "'", gold, result);
    }
  }
  public void testEscapeBR() throws Exception {
    String test = "one<BR class='whatever'>two</\nBR\n>";
    String gold = "one<BR class='whatever'>two</\nBR\n>";
    Set<String> escapedTags = new HashSet<String>(Arrays.asList("BR"));
    Reader reader = new HTMLStripCharFilter
        (new StringReader(test), escapedTags);
    int ch = 0;
    StringBuilder builder = new StringBuilder();
    try {
      while ((ch = reader.read()) != -1){
        builder.append((char)ch);
      }
    } finally {
      // System.out.println("String: " + builder.toString());
    }
    assertEquals("'" + builder.toString() + "' is not equal to '" + gold + "'",
                 gold, builder.toString());
  }
  
  public void testInlineTagsNoSpace() throws Exception {
    String test = "one<sPAn class=\"invisible\">two<sup>2<sup>e</sup></sup>.</SpaN>three";
    String gold = "onetwo2e.three";
    Reader reader = new HTMLStripCharFilter(new StringReader(test));
    int ch = 0;
    StringBuilder builder = new StringBuilder();
    try {
      while ((ch = reader.read()) != -1){
        builder.append((char)ch);
      }
    } finally {
      // System.out.println("String: " + builder.toString());
    }
    assertEquals("'" + builder.toString() + "' is not equal to '" + gold + "'",
                 gold, builder.toString());
  }

  public void testCDATA() throws Exception {
    String test = "one<![CDATA[<one><two>three<four></four></two></one>]]>two";
    String gold = "one<one><two>three<four></four></two></one>two";
    Reader reader = new HTMLStripCharFilter(new StringReader(test));
    int ch = 0;
    StringBuilder builder = new StringBuilder();
    try {
      while ((ch = reader.read()) != -1){
        builder.append((char)ch);
      }
    } finally {
      // System.out.println("String: " + builder.toString());
    }
    assertEquals("'" + builder.toString() + "' is not equal to '" + gold + "'",
                 gold, builder.toString());

    test = "one<![CDATA[two<![CDATA[three]]]]><![CDATA[>four]]>five";
    gold = "onetwo<![CDATA[three]]>fourfive";
    reader = new HTMLStripCharFilter(new StringReader(test));
    ch = 0;
    builder = new StringBuilder();
    try {
      while ((ch = reader.read()) != -1){
        builder.append((char)ch);
      }
    } finally {
      // System.out.println("String: " + builder.toString());
    }
    assertEquals("'" + builder.toString() + "' is not equal to '" + gold + "'",
                 gold, builder.toString());
  }

  public void testUppercaseCharacterEntityVariants() throws Exception {
    String test = " &QUOT;-&COPY;&GT;>&LT;<&REG;&AMP;";
    String gold = " \"-\u00A9>><<\u00AE&";
    Reader reader = new HTMLStripCharFilter(new StringReader(test));
    int ch = 0;
    StringBuilder builder = new StringBuilder();
    try {
      while ((ch = reader.read()) != -1){
        builder.append((char)ch);
      }
    } finally {
      // System.out.println("String: " + builder.toString());
    }
    assertEquals("'" + builder.toString() + "' is not equal to '" + gold + "'",
                 gold, builder.toString());
  }
  
  public void testMSWordMalformedProcessingInstruction() throws Exception {
    String test = "one<?xml:namespace prefix = o ns = \"urn:schemas-microsoft-com:office:office\" />two";
    String gold = "onetwo";
    Reader reader = new HTMLStripCharFilter(new StringReader(test));
    int ch = 0;
    StringBuilder builder = new StringBuilder();
    try {
      while ((ch = reader.read()) != -1){
        builder.append((char)ch);
      }
    } finally {
      // System.out.println("String: " + builder.toString());
    }
    assertEquals("'" + builder.toString() + "' is not equal to '" + gold + "'",
                 gold, builder.toString());
  }

  public void testSupplementaryCharsInTags() throws Exception {
    String test = "one<𩬅艱鍟䇹愯瀛>two<瀛愯𩬅>three 瀛愯𩬅</瀛愯𩬅>four</𩬅艱鍟䇹愯瀛>five<𠀀𠀀>six<𠀀𠀀/>seven";
    String gold = "one\ntwo\nthree 瀛愯𩬅\nfour\nfive\nsix\nseven";
    Reader reader = new HTMLStripCharFilter(new StringReader(test));
    int ch = 0;
    StringBuilder builder = new StringBuilder();
    try {
      while ((ch = reader.read()) != -1){
        builder.append((char)ch);
      }
    } finally {
      // System.out.println("String: " + builder.toString());
    }
    assertEquals("'" + builder.toString() + "' is not equal to '" + gold + "'",
        gold, builder.toString());
  }

  public void testRandomBrokenHTML() throws Exception {
    int maxNumElements = 10000;
    String text = _TestUtil.randomHtmlishString(random(), maxNumElements);
    checkAnalysisConsistency(random(), newTestAnalyzer(), random().nextBoolean(), text);
  }

  public void testRandomText() throws Exception {
    StringBuilder text = new StringBuilder();
    int minNumWords = 10;
    int maxNumWords = 10000;
    int minWordLength = 3;
    int maxWordLength = 20;
    int numWords = _TestUtil.nextInt(random(), minNumWords, maxNumWords);
    switch (_TestUtil.nextInt(random(), 0, 4)) {
      case 0: {
        for (int wordNum = 0 ; wordNum < numWords ; ++wordNum) {
          text.append(_TestUtil.randomUnicodeString(random(), maxWordLength));
          text.append(' ');
        }
        break;
      }
      case 1: {
        for (int wordNum = 0 ; wordNum < numWords ; ++wordNum) {
          text.append(_TestUtil.randomRealisticUnicodeString
              (random(), minWordLength, maxWordLength));
          text.append(' ');
        }
        break;
      }
      default: { // ASCII 50% of the time
        for (int wordNum = 0 ; wordNum < numWords ; ++wordNum) {
          text.append(_TestUtil.randomSimpleString(random()));
          text.append(' ');
        }
      }
    }
    Reader reader = new HTMLStripCharFilter
        (new StringReader(text.toString()));
    while (reader.read() != -1);
  }

  public void testUTF16Surrogates() throws Exception {
    Analyzer analyzer = newTestAnalyzer();
    // Paired surrogates
    assertAnalyzesTo(analyzer, " one two &#xD86C;&#XdC01;three",
        new String[] { "one", "two", "\uD86C\uDC01three" } );
    assertAnalyzesTo(analyzer, " &#55404;&#XdC01;", new String[] { "\uD86C\uDC01" } );
    assertAnalyzesTo(analyzer, " &#xD86C;&#56321;", new String[] { "\uD86C\uDC01" } );
    assertAnalyzesTo(analyzer, " &#55404;&#56321;", new String[] { "\uD86C\uDC01" } );

    // Improperly paired surrogates
    assertAnalyzesTo(analyzer, " &#55404;&#57999;", new String[] { "\uFFFD\uE28F" } );
    assertAnalyzesTo(analyzer, " &#xD86C;&#57999;", new String[] { "\uFFFD\uE28F" } );
    assertAnalyzesTo(analyzer, " &#55002;&#XdC01;", new String[] { "\uD6DA\uFFFD" } );
    assertAnalyzesTo(analyzer, " &#55002;&#56321;", new String[] { "\uD6DA\uFFFD" } );

    // Unpaired high surrogates
    assertAnalyzesTo(analyzer, " &#Xd921;", new String[] { "\uFFFD" } );
    assertAnalyzesTo(analyzer, " &#Xd921", new String[] { "\uFFFD" } );
    assertAnalyzesTo(analyzer, " &#Xd921<br>", new String[] { "&#Xd921" } );
    assertAnalyzesTo(analyzer, " &#55528;", new String[] { "\uFFFD" } );
    assertAnalyzesTo(analyzer, " &#55528", new String[] { "\uFFFD" } );
    assertAnalyzesTo(analyzer, " &#55528<br>", new String[] { "&#55528" } );

    // Unpaired low surrogates
    assertAnalyzesTo(analyzer, " &#xdfdb;", new String[] { "\uFFFD" } );
    assertAnalyzesTo(analyzer, " &#xdfdb", new String[] { "\uFFFD" } );
    assertAnalyzesTo(analyzer, " &#xdfdb<br>", new String[] { "&#xdfdb" } );
    assertAnalyzesTo(analyzer, " &#57209;", new String[] { "\uFFFD" } );
    assertAnalyzesTo(analyzer, " &#57209", new String[] { "\uFFFD" } );
    assertAnalyzesTo(analyzer, " &#57209<br>", new String[] { "&#57209" } );
  }
}
