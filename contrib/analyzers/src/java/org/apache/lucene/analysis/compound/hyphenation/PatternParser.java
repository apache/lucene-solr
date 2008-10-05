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

/* $Id: PatternParser.java 426576 2006-07-28 15:44:37Z jeremias $ */

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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.util.ArrayList;

import javax.xml.parsers.SAXParserFactory;

/**
 * A SAX document handler to read and parse hyphenation patterns from a XML
 * file.
 * 
 * This class has been taken from the Apache FOP project (http://xmlgraphics.apache.org/fop/). They have been slightly modified. 
 */
public class PatternParser extends DefaultHandler implements PatternConsumer {

  XMLReader parser;

  int currElement;

  PatternConsumer consumer;

  StringBuffer token;

  ArrayList exception;

  char hyphenChar;

  String errMsg;

  static final int ELEM_CLASSES = 1;

  static final int ELEM_EXCEPTIONS = 2;

  static final int ELEM_PATTERNS = 3;

  static final int ELEM_HYPHEN = 4;

  public PatternParser() throws HyphenationException {
    token = new StringBuffer();
    parser = createParser();
    parser.setContentHandler(this);
    parser.setErrorHandler(this);
    parser.setEntityResolver(this);
    hyphenChar = '-'; // default

  }

  public PatternParser(PatternConsumer consumer) throws HyphenationException {
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
   * @throws HyphenationException In case of an exception while parsing
   */
  public void parse(String filename) throws HyphenationException {
    parse(new File(filename));
  }

  /**
   * Parses a hyphenation pattern file.
   * 
   * @param file the pattern file
   * @throws HyphenationException In case of an exception while parsing
   */
  public void parse(File file) throws HyphenationException {
    try {
      InputSource src = new InputSource(file.toURL().toExternalForm());
      parse(src);
    } catch (MalformedURLException e) {
      throw new HyphenationException("Error converting the File '" + file
          + "' to a URL: " + e.getMessage());
    }
  }

  /**
   * Parses a hyphenation pattern file.
   * 
   * @param source the InputSource for the file
   * @throws HyphenationException In case of an exception while parsing
   */
  public void parse(InputSource source) throws HyphenationException {
    try {
      parser.parse(source);
    } catch (FileNotFoundException fnfe) {
      throw new HyphenationException("File not found: " + fnfe.getMessage());
    } catch (IOException ioe) {
      throw new HyphenationException(ioe.getMessage());
    } catch (SAXException e) {
      throw new HyphenationException(errMsg);
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
    StringBuffer pat = new StringBuffer();
    int len = word.length();
    for (int i = 0; i < len; i++) {
      if (!Character.isDigit(word.charAt(i))) {
        pat.append(word.charAt(i));
      }
    }
    return pat.toString();
  }

  protected ArrayList normalizeException(ArrayList ex) {
    ArrayList res = new ArrayList();
    for (int i = 0; i < ex.size(); i++) {
      Object item = ex.get(i);
      if (item instanceof String) {
        String str = (String) item;
        StringBuffer buf = new StringBuffer();
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

  protected String getExceptionWord(ArrayList ex) {
    StringBuffer res = new StringBuffer();
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
    StringBuffer il = new StringBuffer();
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
  public InputSource resolveEntity(String publicId, String systemId) {
    return HyphenationDTDGenerator.generateDTD();
  }

  //
  // ContentHandler methods
  //

  /**
   * @see org.xml.sax.ContentHandler#startElement(java.lang.String,
   *      java.lang.String, java.lang.String, org.xml.sax.Attributes)
   */
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
      exception = new ArrayList();
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

  //
  // ErrorHandler methods
  //

  /**
   * @see org.xml.sax.ErrorHandler#warning(org.xml.sax.SAXParseException)
   */
  public void warning(SAXParseException ex) {
    errMsg = "[Warning] " + getLocationString(ex) + ": " + ex.getMessage();
  }

  /**
   * @see org.xml.sax.ErrorHandler#error(org.xml.sax.SAXParseException)
   */
  public void error(SAXParseException ex) {
    errMsg = "[Error] " + getLocationString(ex) + ": " + ex.getMessage();
  }

  /**
   * @see org.xml.sax.ErrorHandler#fatalError(org.xml.sax.SAXParseException)
   */
  public void fatalError(SAXParseException ex) throws SAXException {
    errMsg = "[Fatal Error] " + getLocationString(ex) + ": " + ex.getMessage();
    throw ex;
  }

  /**
   * Returns a string of the location.
   */
  private String getLocationString(SAXParseException ex) {
    StringBuffer str = new StringBuffer();

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

  // PatternConsumer implementation for testing purposes
  public void addClass(String c) {
    System.out.println("class: " + c);
  }

  public void addException(String w, ArrayList e) {
    System.out.println("exception: " + w + " : " + e.toString());
  }

  public void addPattern(String p, String v) {
    System.out.println("pattern: " + p + " : " + v);
  }

  public static void main(String[] args) throws Exception {
    if (args.length > 0) {
      PatternParser pp = new PatternParser();
      pp.setConsumer(pp);
      pp.parse(args[0]);
    }
  }
}

class HyphenationDTDGenerator {
  public static final String DTD_STRING=
    "<?xml version=\"1.0\" encoding=\"US-ASCII\"?>\n"+
    "<!--\n"+
    "  Copyright 1999-2004 The Apache Software Foundation\n"+
    "\n"+
    "  Licensed under the Apache License, Version 2.0 (the \"License\");\n"+
    "  you may not use this file except in compliance with the License.\n"+
    "  You may obtain a copy of the License at\n"+
    "\n"+
    "       http://www.apache.org/licenses/LICENSE-2.0\n"+
    "\n"+
    "  Unless required by applicable law or agreed to in writing, software\n"+
    "  distributed under the License is distributed on an \"AS IS\" BASIS,\n"+
    "  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n"+
    "  See the License for the specific language governing permissions and\n"+
    "  limitations under the License.\n"+
    "-->\n"+
    "<!-- $Id: hyphenation.dtd,v 1.3 2004/02/27 18:34:59 jeremias Exp $ -->\n"+
    "\n"+
    "<!ELEMENT hyphenation-info (hyphen-char?, hyphen-min?,\n"+
    "                           classes, exceptions?, patterns)>\n"+
    "\n"+
    "<!-- Hyphen character to be used in the exception list as shortcut for\n"+
    "     <hyphen pre-break=\"-\"/>. Defaults to '-'\n"+
    "-->\n"+
    "<!ELEMENT hyphen-char EMPTY>\n"+
    "<!ATTLIST hyphen-char value CDATA #REQUIRED>\n"+
    "\n"+
    "<!-- Default minimun length in characters of hyphenated word fragments\n"+
    "     before and after the line break. For some languages this is not\n"+
    "     only for aesthetic purposes, wrong hyphens may be generated if this\n"+
    "     is not accounted for.\n"+
    "-->\n"+
    "<!ELEMENT hyphen-min EMPTY>\n"+
    "<!ATTLIST hyphen-min before CDATA #REQUIRED>\n"+
    "<!ATTLIST hyphen-min after CDATA #REQUIRED>\n"+
    "\n"+
    "<!-- Character equivalent classes: space separated list of character groups, all\n"+
    "     characters in a group are to be treated equivalent as far as\n"+
    "     the hyphenation algorithm is concerned. The first character in a group\n"+
    "     is the group's equivalent character. Patterns should only contain\n"+
    "     first characters. It also defines word characters, i.e. a word that\n"+
    "     contains characters not present in any of the classes is not hyphenated.\n"+
    "-->\n"+
    "<!ELEMENT classes (#PCDATA)>\n"+
    "\n"+
    "<!-- Hyphenation exceptions: space separated list of hyphenated words.\n"+
    "     A hyphen is indicated by the hyphen tag, but you can use the\n"+
    "     hyphen-char defined previously as shortcut. This is in cases\n"+
    "     when the algorithm procedure finds wrong hyphens or you want\n"+
    "     to provide your own hyphenation for some words.\n"+
    "-->\n"+
    "<!ELEMENT exceptions (#PCDATA|hyphen)* >\n"+
    "\n"+
    "<!-- The hyphenation patterns, space separated. A pattern is made of 'equivalent'\n"+
    "     characters as described before, between any two word characters a digit\n"+
    "     in the range 0 to 9 may be specified. The absence of a digit is equivalent\n"+
    "     to zero. The '.' character is reserved to indicate begining or ending\n"+
    "     of words. -->\n"+
    "<!ELEMENT patterns (#PCDATA)>\n"+
    "\n"+
    "<!-- A \"full hyphen\" equivalent to TeX's \\discretionary\n"+
    "     with pre-break, post-break and no-break attributes.\n"+
    "     To be used in the exceptions list, the hyphen character is not\n"+
    "     automatically added -->\n"+
    "<!ELEMENT hyphen EMPTY>\n"+
    "<!ATTLIST hyphen pre CDATA #IMPLIED>\n"+
    "<!ATTLIST hyphen no CDATA #IMPLIED>\n"+
    "<!ATTLIST hyphen post CDATA #IMPLIED>\n";
  
 public static InputSource generateDTD() {
    return new InputSource(new StringReader(DTD_STRING));
  }
}
