/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lucene.analysis.compound.hyphenation;

// SAX
import org.xml.sax.XMLReader;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.Attributes;

// Java
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import javax.xml.parsers.SAXParserFactory;

/**
 * A SAX document handler to read and parse hyphenation patterns from a XML
 * file.
 * 
 * This class has been taken from the Apache FOP project (http://xmlgraphics.apache.org/fop/). They have been slightly modified. 
 */
public class PatternParser extends DefaultHandler {

  XMLReader parser;

  int currElement;

  PatternConsumer consumer;

  StringBuilder token;

  ArrayList<Object> exception;

  char hyphenChar;

  String errMsg;

  static final int ELEM_CLASSES = 1;

  static final int ELEM_EXCEPTIONS = 2;

  static final int ELEM_PATTERNS = 3;

  static final int ELEM_HYPHEN = 4;

  public PatternParser() {
    token = new StringBuilder();
    parser = createParser();
    parser.setContentHandler(this);
    parser.setErrorHandler(this);
    parser.setEntityResolver(this);
    hyphenChar = '-'; // default

  }

  public PatternParser(PatternConsumer consumer) {
    this();
    this.consumer = consumer;
  }

  public void setConsumer(PatternConsumer consumer) {
    this.consumer = consumer;
  }

  /**
   * Parses a hyphenation pattern file.
   * 
   * @param filename the filename
   * @throws IOException In case of an exception while parsing
   */
  public void parse(String filename) throws IOException {
    parse(new InputSource(filename));
  }

  /**
   * Parses a hyphenation pattern file.
   * 
   * @param file the pattern file
   * @throws IOException In case of an exception while parsing
   */
  public void parse(File file) throws IOException {
    InputSource src = new InputSource(file.toURI().toASCIIString());
    parse(src);
  }

  /**
   * Parses a hyphenation pattern file.
   * 
   * @param source the InputSource for the file
   * @throws IOException In case of an exception while parsing
   */
  public void parse(InputSource source) throws IOException {
    try {
      parser.parse(source);
    } catch (SAXException e) {
      throw new IOException(e);
    }
  }

  /**
   * Creates a SAX parser using JAXP
   * 
   * @return the created SAX parser
   */
  static XMLReader createParser() {
    try {
      SAXParserFactory factory = SAXParserFactory.newInstance();
      factory.setNamespaceAware(true);
      return factory.newSAXParser().getXMLReader();
    } catch (Exception e) {
      throw new RuntimeException("Couldn't create XMLReader: " + e.getMessage());
    }
  }

  protected String readToken(StringBuffer chars) {
    String word;
    boolean space = false;
    int i;
    for (i = 0; i < chars.length(); i++) {
      if (Character.isWhitespace(chars.charAt(i))) {
        space = true;
      } else {
        break;
      }
    }
    if (space) {
      // chars.delete(0,i);
      for (int countr = i; countr < chars.length(); countr++) {
        chars.setCharAt(countr - i, chars.charAt(countr));
      }
      chars.setLength(chars.length() - i);
      if (token.length() > 0) {
        word = token.toString();
        token.setLength(0);
        return word;
      }
    }
    space = false;
    for (i = 0; i < chars.length(); i++) {
      if (Character.isWhitespace(chars.charAt(i))) {
        space = true;
        break;
      }
    }
    token.append(chars.toString().substring(0, i));
    // chars.delete(0,i);
    for (int countr = i; countr < chars.length(); countr++) {
      chars.setCharAt(countr - i, chars.charAt(countr));
    }
    chars.setLength(chars.length() - i);
    if (space) {
      word = token.toString();
      token.setLength(0);
      return word;
    }
    token.append(chars);
    return null;
  }

  protected static String getPattern(String word) {
    StringBuilder pat = new StringBuilder();
    int len = word.length();
    for (int i = 0; i < len; i++) {
      if (!Character.isDigit(word.charAt(i))) {
        pat.append(word.charAt(i));
      }
    }
    return pat.toString();
  }

  protected ArrayList<Object> normalizeException(ArrayList<?> ex) {
    ArrayList<Object> res = new ArrayList<Object>();
    for (int i = 0; i < ex.size(); i++) {
      Object item = ex.get(i);
      if (item instanceof String) {
        String str = (String) item;
        StringBuilder buf = new StringBuilder();
        for (int j = 0; j < str.length(); j++) {
          char c = str.charAt(j);
          if (c != hyphenChar) {
            buf.append(c);
          } else {
            res.add(buf.toString());
            buf.setLength(0);
            char[] h = new char[1];
            h[0] = hyphenChar;
            // we use here hyphenChar which is not necessarily
            // the one to be printed
            res.add(new Hyphen(new String(h), null, null));
          }
        }
        if (buf.length() > 0) {
          res.add(buf.toString());
        }
      } else {
        res.add(item);
      }
    }
    return res;
  }

  protected String getExceptionWord(ArrayList<?> ex) {
    StringBuilder res = new StringBuilder();
    for (int i = 0; i < ex.size(); i++) {
      Object item = ex.get(i);
      if (item instanceof String) {
        res.append((String) item);
      } else {
        if (((Hyphen) item).noBreak != null) {
          res.append(((Hyphen) item).noBreak);
        }
      }
    }
    return res.toString();
  }

  protected static String getInterletterValues(String pat) {
    StringBuilder il = new StringBuilder();
    String word = pat + "a"; // add dummy letter to serve as sentinel
    int len = word.length();
    for (int i = 0; i < len; i++) {
      char c = word.charAt(i);
      if (Character.isDigit(c)) {
        il.append(c);
        i++;
      } else {
        il.append('0');
      }
    }
    return il.toString();
  }

  //
  // EntityResolver methods
  //
  @Override
  public InputSource resolveEntity(String publicId, String systemId) {
    // supply the internal hyphenation.dtd if possible
    if (
      (systemId != null && systemId.matches("(?i).*\\bhyphenation.dtd\\b.*")) ||
      ("hyphenation-info".equals(publicId))
    ) {
      // System.out.println(this.getClass().getResource("hyphenation.dtd").toExternalForm());
      return new InputSource(this.getClass().getResource("hyphenation.dtd").toExternalForm());
    }
    return null;
  }

  //
  // ContentHandler methods
  //

  /**
   * @see org.xml.sax.ContentHandler#startElement(java.lang.String,
   *      java.lang.String, java.lang.String, org.xml.sax.Attributes)
   */
  @Override
  public void startElement(String uri, String local, String raw,
      Attributes attrs) {
    if (local.equals("hyphen-char")) {
      String h = attrs.getValue("value");
      if (h != null && h.length() == 1) {
        hyphenChar = h.charAt(0);
      }
    } else if (local.equals("classes")) {
      currElement = ELEM_CLASSES;
    } else if (local.equals("patterns")) {
      currElement = ELEM_PATTERNS;
    } else if (local.equals("exceptions")) {
      currElement = ELEM_EXCEPTIONS;
      exception = new ArrayList<Object>();
    } else if (local.equals("hyphen")) {
      if (token.length() > 0) {
        exception.add(token.toString());
      }
      exception.add(new Hyphen(attrs.getValue("pre"), attrs.getValue("no"),
          attrs.getValue("post")));
      currElement = ELEM_HYPHEN;
    }
    token.setLength(0);
  }

  /**
   * @see org.xml.sax.ContentHandler#endElement(java.lang.String,
   *      java.lang.String, java.lang.String)
   */
  @Override
  @SuppressWarnings("unchecked")
  public void endElement(String uri, String local, String raw) {

    if (token.length() > 0) {
      String word = token.toString();
      switch (currElement) {
        case ELEM_CLASSES:
          consumer.addClass(word);
          break;
        case ELEM_EXCEPTIONS:
          exception.add(word);
          exception = normalizeException(exception);
          consumer.addException(getExceptionWord(exception), 
              (ArrayList) exception.clone());
          break;
        case ELEM_PATTERNS:
          consumer.addPattern(getPattern(word), getInterletterValues(word));
          break;
        case ELEM_HYPHEN:
          // nothing to do
          break;
      }
      if (currElement != ELEM_HYPHEN) {
        token.setLength(0);
      }
    }
    if (currElement == ELEM_HYPHEN) {
      currElement = ELEM_EXCEPTIONS;
    } else {
      currElement = 0;
    }

  }

  /**
   * @see org.xml.sax.ContentHandler#characters(char[], int, int)
   */
  @SuppressWarnings("unchecked")
  @Override
  public void characters(char ch[], int start, int length) {
    StringBuffer chars = new StringBuffer(length);
    chars.append(ch, start, length);
    String word = readToken(chars);
    while (word != null) {
      // System.out.println("\"" + word + "\"");
      switch (currElement) {
        case ELEM_CLASSES:
          consumer.addClass(word);
          break;
        case ELEM_EXCEPTIONS:
          exception.add(word);
          exception = normalizeException(exception);
          consumer.addException(getExceptionWord(exception),
              (ArrayList) exception.clone());
          exception.clear();
          break;
        case ELEM_PATTERNS:
          consumer.addPattern(getPattern(word), getInterletterValues(word));
          break;
      }
      word = readToken(chars);
    }

  }

  /**
   * Returns a string of the location.
   */
  private String getLocationString(SAXParseException ex) {
    StringBuilder str = new StringBuilder();

    String systemId = ex.getSystemId();
    if (systemId != null) {
      int index = systemId.lastIndexOf('/');
      if (index != -1) {
        systemId = systemId.substring(index + 1);
      }
      str.append(systemId);
    }
    str.append(':');
    str.append(ex.getLineNumber());
    str.append(':');
    str.append(ex.getColumnNumber());

    return str.toString();

  } // getLocationString(SAXParseException):String
}
