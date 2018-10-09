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
package org.apache.solr.util;

import java.io.FilterReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.input.CloseShieldInputStream;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.solr.common.EmptyEntityResolver;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.common.util.XMLErrorLogger;
import org.slf4j.Logger;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * Some utility methods for parsing XML in a safe way. This class can be used to parse XML
 * coming from network (completely untrusted) or it can load a config file from a
 * {@link ResourceLoader}. In this case it allows external entities and xincludes, but only
 * referring to files reachable by the loader.
 */
@SuppressForbidden(reason = "This class uses XML APIs directly that should not be used anywhere else in Solr code")
public final class SafeXMLParsing  {
  
  public static final String SYSTEMID_UNTRUSTED = "untrusted://stream";

  private SafeXMLParsing() {}
  
  /** Parses a config file from ResourceLoader. Xinclude and external entities are enabled, but cannot escape the resource loader. */
  public static Document parseConfigXML(Logger log, ResourceLoader loader, String file) throws SAXException, IOException {
    try (InputStream in = loader.openResource(file)) {
      final DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
      dbf.setValidating(false);
      dbf.setNamespaceAware(true);
      trySetDOMFeature(dbf, XMLConstants.FEATURE_SECURE_PROCESSING, true);
      try {
        dbf.setXIncludeAware(true);
      } catch (UnsupportedOperationException e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "XML parser doesn't support XInclude option", e);
      }
      
      final DocumentBuilder db = dbf.newDocumentBuilder();
      db.setEntityResolver(new SystemIdResolver(loader));
      db.setErrorHandler(new XMLErrorLogger(log));
      return db.parse(in, SystemIdResolver.createSystemIdFromResourceName(file));
    } catch (ParserConfigurationException pce) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "XML parser cannot be configured", pce);
    }
  }

  /** Parses the given InputStream as XML, disabling any external entities with secure processing enabled.
   * The given InputStream is not closed. */
  public static Document parseUntrustedXML(Logger log, InputStream in) throws SAXException, IOException {
    return getUntrustedDocumentBuilder(log).parse(new CloseShieldInputStream(in), SYSTEMID_UNTRUSTED);
  }
  
  /** Parses the given InputStream as XML, disabling any external entities with secure processing enabled.
   * The given Reader is not closed. */
  public static Document parseUntrustedXML(Logger log, Reader reader) throws SAXException, IOException {
    final InputSource is = new InputSource(new FilterReader(reader) {
      @Override public void close() {}
    });
    is.setSystemId(SYSTEMID_UNTRUSTED);
    return getUntrustedDocumentBuilder(log).parse(is);
  }
  
  public static Document parseUntrustedXML(Logger log, String xml) throws SAXException, IOException {
    return parseUntrustedXML(log, new StringReader(xml));
  }

  private static DocumentBuilder getUntrustedDocumentBuilder(Logger log) {
    try {
      final DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
      dbf.setValidating(false);
      dbf.setNamespaceAware(true);
      trySetDOMFeature(dbf, XMLConstants.FEATURE_SECURE_PROCESSING, true);
      
      final DocumentBuilder db = dbf.newDocumentBuilder();
      db.setEntityResolver(EmptyEntityResolver.SAX_INSTANCE);
      db.setErrorHandler(new XMLErrorLogger(log));
      return db;
    } catch (ParserConfigurationException pce) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "XML parser cannot be configured", pce);
    }
  }
  
  private static void trySetDOMFeature(DocumentBuilderFactory factory, String feature, boolean enabled) {
    try {
      factory.setFeature(feature, enabled);
    } catch (Exception ex) {
      // ignore
    }
  }
  
}
