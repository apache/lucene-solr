package org.apache.lucenesandbox.xmlindexingdemo;

import org.xml.sax.*;
import javax.xml.parsers.*;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import java.io.File;
import java.io.IOException;

public class XMLDocumentHandlerSAX
  extends HandlerBase {
  /** A buffer for each XML element */
  private StringBuffer elementBuffer = new StringBuffer();

  private Document mDocument;

  // constructor
  public XMLDocumentHandlerSAX(File xmlFile)
    throws ParserConfigurationException, SAXException, IOException {
    SAXParserFactory spf = SAXParserFactory.newInstance();

    SAXParser parser = spf.newSAXParser();
    parser.parse(xmlFile, this);
  }

  // call at document start
  public void startDocument() {
    mDocument = new Document();
  }

  // call at element start
  public void startElement(String localName, AttributeList atts)
    throws SAXException {
    elementBuffer.setLength(0);
  }

  // call when cdata found
  public void characters(char[] text, int start, int length) {
    elementBuffer.append(text, start, length);
  }

  // call at element end
  public void endElement(String localName)
    throws SAXException {
    mDocument.add(Field.Text(localName, elementBuffer.toString()));
  }

  public Document getDocument() {
    return mDocument;
  }
}
