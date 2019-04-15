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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;

import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.solr.SolrTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;

public class TestSafeXMLParsing extends SolrTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  public void testUntrusted() throws Exception {
    // TODO: Fix the underlying EmptyEntityResolver to not replace external entities by nothing and instead throw exception:
    Document doc = SafeXMLParsing.parseUntrustedXML(log, "<!DOCTYPE test [\n" + 
        "<!ENTITY internalTerm \"foobar\">\n" + 
        "<!ENTITY externalTerm SYSTEM \"foo://bar.xyz/external\">\n" + 
        "]>\n" + 
        "<test>&internalTerm;&externalTerm;</test>");
    assertEquals("foobar", doc.getDocumentElement().getTextContent());
  }
  
  InputStream getStringStream(String xml) {
    return new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8));
  }
  
  public void testConfig() throws Exception {
    final ResourceLoader loader = new ResourceLoader() {
      @Override
      public InputStream openResource(String resource) throws IOException {
        switch (resource) {
          case "source1.xml":
            return getStringStream("<!DOCTYPE test [\n" + 
                "<!ENTITY externalTerm SYSTEM \"foo://bar.xyz/external\">\n" + 
                "]>\n" + 
                "<test>&externalTerm;</test>");
          case "source2.xml":
            return getStringStream("<!DOCTYPE test [\n" + 
                "<!ENTITY externalTerm SYSTEM \"./include1.xml\">\n" + 
                "]>\n" + 
                "<test>&externalTerm;</test>");
          case "source3.xml":
            return getStringStream("<foo xmlns:xi=\"http://www.w3.org/2001/XInclude\">\n" + 
                "  <xi:include href=\"./include2.xml\"/>\n" + 
                "</foo>");
          case "include1.xml":
            return getStringStream("Make XML Great Again!™");
          case "include2.xml":
            return getStringStream("<bar>Make XML Great Again!™</bar>");
        }
        throw new IOException("Resource not found: " + resource);
      }

      @Override
      public <T> Class<? extends T> findClass(String cname, Class<T> expectedType) {
        throw new UnsupportedOperationException();
      }

      @Override
      public <T> T newInstance(String cname, Class<T> expectedType) {
        throw new UnsupportedOperationException();
      }
      
    };
    
    IOException ioe = expectThrows(IOException.class, () -> {
      SafeXMLParsing.parseConfigXML(log, loader, "source1.xml");
    });
    assertTrue(ioe.getMessage().contains("Cannot resolve absolute systemIDs"));
    
    Document doc = SafeXMLParsing.parseConfigXML(log, loader, "source2.xml");
    assertEquals("Make XML Great Again!™", doc.getDocumentElement().getTextContent());
    
    doc = SafeXMLParsing.parseConfigXML(log, loader, "source3.xml");
    assertEquals("Make XML Great Again!™", doc.getDocumentElement().getTextContent().trim());
  }

}
