package org.apache.lucene.benchmark.byTask.feeds;

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
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;

import org.cyberneko.html.parsers.SAXParser;

import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Simple HTML Parser extracting title, meta tags, and body text
 * that is based on <a href="http://nekohtml.sourceforge.net/">NekoHTML</a>.
 */
public class DemoHTMLParser implements HTMLParser {
  
  /** The actual parser to read HTML documents */
  public static final class Parser {
    
    public final Properties metaTags = new Properties();
    public final String title, body;
    
    public Parser(Reader reader) throws IOException, SAXException {
      this(new InputSource(reader));
    }
    
    public Parser(InputSource source) throws IOException, SAXException {
      final SAXParser parser = new SAXParser();
      parser.setFeature("http://xml.org/sax/features/namespaces", true);
      parser.setFeature("http://cyberneko.org/html/features/balance-tags", true);
      parser.setFeature("http://cyberneko.org/html/features/report-errors", false);
      parser.setProperty("http://cyberneko.org/html/properties/names/elems", "lower");
      parser.setProperty("http://cyberneko.org/html/properties/names/attrs", "lower");

      final StringBuilder title = new StringBuilder(), body = new StringBuilder();
      final DefaultHandler handler = new DefaultHandler() {
        private int inBODY = 0, inHEAD = 0, inTITLE = 0, suppressed = 0;

        @Override
        public void startElement(String namespaceURI, String localName, String qName, Attributes atts) throws SAXException {
          if (inHEAD > 0) {
            if ("title".equals(localName)) {
              inTITLE++;
            } else {
              if ("meta".equals(localName)) {
                String name = atts.getValue("name");
                if (name == null) {
                  name = atts.getValue("http-equiv");
                }
                final String val = atts.getValue("content");
                if (name != null && val != null) {
                  metaTags.setProperty(name.toLowerCase(Locale.ROOT), val);
                }
              }
            }
          } else if (inBODY > 0) {
            if (SUPPRESS_ELEMENTS.contains(localName)) {
              suppressed++;
            } else if ("img".equals(localName)) {
              // the original javacc-based parser preserved <IMG alt="..."/>
              // attribute as body text in [] parenthesis:
              final String alt = atts.getValue("alt");
              if (alt != null) {
                body.append('[').append(alt).append(']');
              }
            }
          } else if ("body".equals(localName)) {
            inBODY++;
          } else if ("head".equals(localName)) {
            inHEAD++;
          } else if ("frameset".equals(localName)) {
            throw new SAXException("This parser does not support HTML framesets.");
          }
        }

        @Override
        public void endElement(String namespaceURI, String localName, String qName) throws SAXException {
          if (inBODY > 0) {
            if ("body".equals(localName)) {
              inBODY--;
            } else if (ENDLINE_ELEMENTS.contains(localName)) {
              body.append('\n');
            } else if (SUPPRESS_ELEMENTS.contains(localName)) {
              suppressed--;
            }
          } else if (inHEAD > 0) {
            if ("head".equals(localName)) {
              inHEAD--;
            } else if (inTITLE > 0 && "title".equals(localName)) {
              inTITLE--;
            }
          }
        }
        
        @Override
        public void characters(char[] ch, int start, int length) throws SAXException { 
          if (inBODY > 0 && suppressed == 0) {
            body.append(ch, start, length);
          } else if (inTITLE > 0) {
            title.append(ch, start, length);
          }
        }

        @Override
        public InputSource resolveEntity(String publicId, String systemId) {
          // disable network access caused by DTDs
          return new InputSource(new StringReader(""));
        }
      };
      
      parser.setContentHandler(handler);
      parser.setErrorHandler(handler);
      parser.parse(source);
      
      // the javacc-based parser trimmed title (which should be done for HTML in all cases):
      this.title = title.toString().trim();
      
      // assign body text
      this.body = body.toString();
    }
    
    private static final Set<String> createElementNameSet(String... names) {
      return Collections.unmodifiableSet(new HashSet<String>(Arrays.asList(names)));
    }
    
    /** HTML elements that cause a line break (they are block-elements) */
    static final Set<String> ENDLINE_ELEMENTS = createElementNameSet(
      "p", "h1", "h2", "h3", "h4", "h5", "h6", "div", "ul", "ol", "dl",
      "pre", "hr", "blockquote", "address", "fieldset", "table", "form",
      "noscript", "li", "dt", "dd", "noframes", "br", "tr", "select", "option"
    );

    /** HTML elements with contents that are ignored */
    static final Set<String> SUPPRESS_ELEMENTS = createElementNameSet(
      "style", "script"
    );
  }

  @Override
  public DocData parse(DocData docData, String name, Date date, Reader reader, TrecContentSource trecSrc) throws IOException {
    try {
      return parse(docData, name, date, new InputSource(reader), trecSrc);
    } catch (SAXException saxe) {
      throw new IOException("SAX exception occurred while parsing HTML document.", saxe);
    }
  }
  
  public DocData parse(DocData docData, String name, Date date, InputSource source, TrecContentSource trecSrc) throws IOException, SAXException {
    final Parser p = new Parser(source);
    
    // properties 
    final Properties props = p.metaTags;
    String dateStr = props.getProperty("date");
    if (dateStr != null) {
      final Date newDate = trecSrc.parseDate(dateStr);
      if (newDate != null) {
        date = newDate;
      }
    }
    
    docData.clear();
    docData.setName(name);
    docData.setBody(p.body);
    docData.setTitle(p.title);
    docData.setProps(props);
    docData.setDate(date);
    return docData;
  }

}
