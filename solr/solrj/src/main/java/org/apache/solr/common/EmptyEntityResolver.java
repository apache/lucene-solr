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
package org.apache.solr.common;

import java.io.InputStream;
import org.xml.sax.InputSource;
import org.xml.sax.EntityResolver;
import javax.xml.XMLConstants;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLResolver;

import org.apache.commons.io.input.ClosedInputStream;

/**
 * This class provides several singletons of entity resolvers used by
 * SAX and StAX in the Java API. This is needed to make secure
 * XML parsers, that don't resolve external entities from untrusted sources.
 * <p>This class also provides static methods to configure SAX and StAX
 * parsers to be safe.
 * <p>Parsers will get an empty, closed stream for every external
 * entity, so they will not fail while parsing (unless the external entity
 * is needed for processing!).
 */
public final class EmptyEntityResolver {

  public static final EntityResolver SAX_INSTANCE = new EntityResolver() {
    @Override
    public InputSource resolveEntity(String publicId, String systemId) {
      return new InputSource(ClosedInputStream.CLOSED_INPUT_STREAM);
    }
  };

  public static final XMLResolver STAX_INSTANCE = new XMLResolver() {
    @Override
    public InputStream resolveEntity(String publicId, String systemId, String baseURI, String namespace) {
      return ClosedInputStream.CLOSED_INPUT_STREAM;
    }
  };
  
  // no instance!
  private EmptyEntityResolver() {}
  
  private static void trySetSAXFeature(SAXParserFactory saxFactory, String feature, boolean enabled) {
    try {
      saxFactory.setFeature(feature, enabled);
    } catch (Exception ex) {
      // ignore
    }
  }
  
  /** Configures the given {@link SAXParserFactory} to do secure XML processing of untrusted sources.
   * It is required to also set {@link #SAX_INSTANCE} on the created {@link org.xml.sax.XMLReader}.
   * @see #SAX_INSTANCE
   */
  public static void configureSAXParserFactory(SAXParserFactory saxFactory) {
    // don't enable validation of DTDs:
    saxFactory.setValidating(false);
    // enable secure processing:
    trySetSAXFeature(saxFactory, XMLConstants.FEATURE_SECURE_PROCESSING, true);
  }
  
  private static void trySetStAXProperty(XMLInputFactory inputFactory, String key, Object value) {
    try {
      inputFactory.setProperty(key, value);
    } catch (Exception ex) {
      // ignore
    }
  }
  
  /** Configures the given {@link XMLInputFactory} to not parse external entities.
   * No further configuration on is needed, all required entity resolvers are configured.
   */
  public static void configureXMLInputFactory(XMLInputFactory inputFactory) {
    // don't enable validation of DTDs:
    trySetStAXProperty(inputFactory, XMLInputFactory.IS_VALIDATING, Boolean.FALSE);
    // enable this to *not* produce parsing failure on external entities:
    trySetStAXProperty(inputFactory, XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, Boolean.TRUE);
    inputFactory.setXMLResolver(EmptyEntityResolver.STAX_INSTANCE);
  }
  
}
