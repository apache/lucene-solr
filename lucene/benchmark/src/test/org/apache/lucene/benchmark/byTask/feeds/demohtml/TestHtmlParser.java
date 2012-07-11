package org.apache.lucene.benchmark.byTask.feeds.demohtml;

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

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Properties;

import org.apache.lucene.util.LuceneTestCase;

public class TestHtmlParser extends LuceneTestCase {

  public void testUnicode() throws Exception {
    String text = "<html><body>汉语</body></html>";
    HTMLParser parser = new HTMLParser(new StringReader(text));
    assertReadsTo("汉语", parser);
  }
  
  public void testEntities() throws Exception {
    String text = "<html><body>&#x6C49;&#x8BED;&yen;</body></html>";
    HTMLParser parser = new HTMLParser(new StringReader(text));
    assertReadsTo("汉语¥", parser);
  }
  
  public void testComments() throws Exception {
    String text = "<html><body>foo<!-- bar --><! baz --></body></html>";
    HTMLParser parser = new HTMLParser(new StringReader(text));
    assertReadsTo("foo", parser);
  }
  
  public void testScript() throws Exception {
    String text = "<html><body><script type=\"text/javascript\">" +
                  "document.write(\"test\")</script>foo</body></html>"; 
    HTMLParser parser = new HTMLParser(new StringReader(text));
    assertReadsTo("foo", parser);
  }
  
  public void testStyle() throws Exception {
    String text = "<html><head><style type=\"text/css\">" +
                  "body{background-color:blue;}</style>" +
                  "</head><body>foo</body></html>";
    HTMLParser parser = new HTMLParser(new StringReader(text));
    assertReadsTo("foo", parser);
  }
  
  public void testDoctype() throws Exception {
    String text = "<!DOCTYPE HTML PUBLIC " + 
    "\"-//W3C//DTD HTML 4.01 Transitional//EN\"" +
    "\"http://www.w3.org/TR/html4/loose.dtd\">" +
    "<html><body>foo</body></html>";
    HTMLParser parser = new HTMLParser(new StringReader(text));
    assertReadsTo("foo", parser);
  }
  
  public void testMeta() throws Exception {
    String text = "<html><head>" +
    "<meta name=\"a\" content=\"1\" />" +
    "<meta name=\"b\" content=\"2\" />" +
    "<meta name=\"keywords\" content=\"this is a test\" />" +
    "<meta http-equiv=\"Content-Type\" content=\"text/html;charset=UTF-8\" />" +
    "</head><body>foobar</body></html>";
    HTMLParser parser = new HTMLParser(new StringReader(text));
    Properties tags = parser.getMetaTags();
    assertEquals(4, tags.size());
    assertEquals("1", tags.get("a"));
    assertEquals("2", tags.get("b"));
    assertEquals("this is a test", tags.get("keywords"));
    assertEquals("text/html;charset=utf-8", tags.get("content-type"));
  }
  
  public void testTitle() throws Exception {
    String text = "<html><head><TITLE>foo</TITLE><head><body>bar</body></html>";
    HTMLParser parser = new HTMLParser(new StringReader(text));
    assertEquals("foo", parser.getTitle());
  }
  
  public void testSummary() throws Exception {
    String text = "<html><head><TITLE>foo</TITLE><head><body>" + 
    "Summarize me. Summarize me. Summarize me. Summarize me. " + 
    "Summarize me. Summarize me. Summarize me. Summarize me. " + 
    "Summarize me. Summarize me. Summarize me. Summarize me. " + 
    "Summarize me. Summarize me. Summarize me. Summarize me. " + 
    "Summarize me. Summarize me. Summarize me. Summarize me. " + 
    "Summarize me. Summarize me. Summarize me. Summarize me. " + 
    "Summarize me. Summarize me. Summarize me. Summarize me. " + 
    "</body></html>";
    HTMLParser parser = new HTMLParser(new StringReader(text));
    assertEquals(200, parser.getSummary().length());
  }
  
  // LUCENE-590
  public void testSummaryTitle() throws Exception {
    String text = "<html><head><title>Summary</title></head><body>Summary of the document</body></html>";
    HTMLParser parser = new HTMLParser(new StringReader(text));
    assertEquals("Summary of the document", parser.getSummary());
  }
  
  // LUCENE-2246
  public void testTurkish() throws Exception {
    String text = "<html><body>" +
    "<IMG SRC=\"../images/head.jpg\" WIDTH=570 HEIGHT=47 BORDER=0 ALT=\"ş\">" +
    "<a title=\"(ııı)\"></body></html>";
    HTMLParser parser = new HTMLParser(new StringReader(text));
    assertReadsTo("[ş]", parser);
  }
  
  private void assertReadsTo(String expected, HTMLParser parser) throws IOException {
    Reader reader = parser.getReader();
    StringBuilder builder = new StringBuilder();
    int ch = 0;
    while ((ch = reader.read()) != -1) {
      builder.append((char)ch);
    }
    assertEquals(expected, builder.toString());
  }
}
