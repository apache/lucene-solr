package org.apache.lucenesandbox.xmlindexingdemo;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import java.io.File;
import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class XMLDocumentHandlerSAX extends DefaultHandler {
  /** A buffer for each XML element */
  private StringBuffer elementBuffer = new StringBuffer();

  private Document mDocument;

  // constructor
  public XMLDocumentHandlerSAX(File xmlFile)
    throws ParserConfigurationException, SAXException, IOException {
    SAXParserFactory spf = SAXParserFactory.newInstance();

    // use validating parser?
    //spf.setValidating(false);
    // make parser name space aware?
    //spf.setNamespaceAware(true);

    SAXParser parser = spf.newSAXParser();
    //System.out.println("parser is validating: " + parser.isValidating());
    try {
      parser.parse(xmlFile, this);
    } catch (org.xml.sax.SAXParseException spe) {
      System.out.println("SAXParser caught SAXParseException at line: " +
        spe.getLineNumber() + " column " +
        spe.getColumnNumber());
    }
  }

  // call at document start
  public void startDocument() throws SAXException {
    mDocument = new Document();
  }

  // call at element start
  public void startElement(String namespaceURI, String localName,
    String qualifiedName, Attributes attrs) throws SAXException {

    String eName = localName;
     if ("".equals(eName)) {
       eName = qualifiedName; // namespaceAware = false
     }
     // list the attribute(s)
     if (attrs != null) {
       for (int i = 0; i < attrs.getLength(); i++) {
         String aName = attrs.getLocalName(i); // Attr name
         if ("".equals(aName)) { aName = attrs.getQName(i); }
         // perform application specific action on attribute(s)
         // for now just dump out attribute name and value
         System.out.println("attr " + aName+"="+attrs.getValue(i));
       }
     }
     elementBuffer.setLength(0);
  }

  // call when cdata found
  public void characters(char[] text, int start, int length)
    throws SAXException {
    elementBuffer.append(text, start, length);
  }

  // call at element end
  public void endElement(String namespaceURI, String simpleName,
    String qualifiedName)  throws SAXException {

    String eName = simpleName;
    if ("".equals(eName)) {
      eName = qualifiedName; // namespaceAware = false
    }

    mDocument.add(Field.Text(eName, elementBuffer.toString()));
  }

  public Document getDocument() {
    return mDocument;
  }
}
