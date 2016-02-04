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
package org.apache.lucene.benchmark.byTask.feeds;


import java.io.StringReader;
import java.util.Locale;
import java.util.Properties;

import org.apache.lucene.benchmark.byTask.feeds.DemoHTMLParser.Parser;
import org.apache.lucene.util.LuceneTestCase;

public class TestHtmlParser extends LuceneTestCase {

  public void testUnicode() throws Exception {
    String text = "<html><body>汉语</body></html>";
    Parser parser = new Parser(new StringReader(text));
    assertEquals("汉语", parser.body);
  }
  
  public void testEntities() throws Exception {
    String text = "<html><body>&#x6C49;&#x8BED;&yen;</body></html>";
    Parser parser = new Parser(new StringReader(text));
    assertEquals("汉语¥", parser.body);
  }
  
  public void testComments() throws Exception {
    String text = "<html><body>foo<!-- bar --><! baz --></body></html>";
    Parser parser = new Parser(new StringReader(text));
    assertEquals("foo", parser.body);
  }
  
  public void testScript() throws Exception {
    String text = "<html><body><script type=\"text/javascript\">" +
                  "document.write(\"test\")</script>foo</body></html>"; 
    Parser parser = new Parser(new StringReader(text));
    assertEquals("foo", parser.body);
  }
  
  public void testStyle() throws Exception {
    String text = "<html><head><style type=\"text/css\">" +
                  "body{background-color:blue;}</style>" +
                  "</head><body>foo</body></html>";
    Parser parser = new Parser(new StringReader(text));
    assertEquals("foo", parser.body);
  }
  
  public void testDoctype() throws Exception {
    String text = "<!DOCTYPE HTML PUBLIC " + 
    "\"-//W3C//DTD HTML 4.01 Transitional//EN\"" +
    "\"http://www.w3.org/TR/html4/loose.dtd\">" +
    "<html><body>foo</body></html>";
    Parser parser = new Parser(new StringReader(text));
    assertEquals("foo", parser.body);
  }
  
  public void testMeta() throws Exception {
    String text = "<html><head>" +
    "<meta name=\"a\" content=\"1\" />" +
    "<meta name=\"b\" content=\"2\" />" +
    "<meta name=\"keywords\" content=\"this is a test\" />" +
    "<meta http-equiv=\"Content-Type\" content=\"text/html;charset=UTF-8\" />" +
    "</head><body>foobar</body></html>";
    Parser parser = new Parser(new StringReader(text));
    Properties tags = parser.metaTags;
    assertEquals(4, tags.size());
    assertEquals("1", tags.get("a"));
    assertEquals("2", tags.get("b"));
    assertEquals("this is a test", tags.get("keywords"));
    assertEquals("text/html;charset=UTF-8", tags.get("content-type"));
  }
  
  public void testTitle() throws Exception {
    String text = "<html><head><TITLE>foo</TITLE><head><body>bar</body></html>";
    Parser parser = new Parser(new StringReader(text));
    assertEquals("foo", parser.title);
  }
  
  // LUCENE-2246
  public void testTurkish() throws Exception {
    final Locale saved = Locale.getDefault();
    try {
      Locale.setDefault(new Locale("tr", "TR"));
      String text = "<html><HEAD><TITLE>ııı</TITLE></head><body>" +
      "<IMG SRC=\"../images/head.jpg\" WIDTH=570 HEIGHT=47 BORDER=0 ALT=\"ş\">" +
      "<a title=\"(ııı)\"></body></html>";
      Parser parser = new Parser(new StringReader(text));
      assertEquals("ııı", parser.title);
      assertEquals("[ş]", parser.body);
    } finally {
      Locale.setDefault(saved);
    }
  }
  
  public void testSampleTRECDoc() throws Exception {
    String text = "<html>\r\n" + 
        "\r\n" + 
        "<head>\r\n" + 
        "<title>\r\n" + 
        "TEST-000 title\r\n" + 
        "</title>\r\n" + 
        "</head>\r\n" + 
        "\r\n" + 
        "<body>\r\n" + 
        "TEST-000 text\r\n" + 
        "\r\n" + 
        "</body>\r\n" + 
        "\r\n";
    Parser parser = new Parser(new StringReader(text));
    assertEquals("TEST-000 title", parser.title);
    assertEquals("TEST-000 text", parser.body.trim());
  }
  
  public void testNoHTML() throws Exception {
    String text = "hallo";
    Parser parser = new Parser(new StringReader(text));
    assertEquals("", parser.title);
    assertEquals("hallo", parser.body);
  }
  
  public void testivalid() throws Exception {
    String text = "<title>foo</title>bar";
    Parser parser = new Parser(new StringReader(text));
    assertEquals("foo", parser.title);
    assertEquals("bar", parser.body);
  }
  
}
